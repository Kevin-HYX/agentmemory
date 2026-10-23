import type { ISdk } from "iii-sdk";
import type {
  GraphNode,
  GraphEdge,
  GraphQueryResult,
  GraphSnapshot,
  CompressedObservation,
  MemoryProvider,
} from "../types.js";
import { KV, generateId } from "../state/schema.js";
import type { StateKV } from "../state/kv.js";
import {
  GRAPH_EXTRACTION_SYSTEM,
  buildGraphExtractionPrompt,
} from "../prompts/graph-extraction.js";
import { isGraphExtractionEnabled } from "../config.js";
import { recordAudit } from "./audit.js";
import { logger } from "../logger.js";
import {
  OBS_INDEX_CAP,
  hydrateEdgesObs,
  hydrateNodesObs,
  indexObsToNodes,
  readEdgeObsMap,
  readNodeObsMap,
  stripEdgeObs,
  stripNodeObs,
  unionObs,
  writeEdgeObsMany,
  writeNodeObsMany,
} from "../state/graph-provenance.js";

// #753: keep the response payload below the iii state channel ceiling.
// 500 nodes + their incident edges hold well under the limit on the
// reported 11k-node / 28k-edge corpus, and 5,000 is the upper bound a
// caller can request explicitly. Tuned conservatively because edges
// fan out faster than nodes.
const DEFAULT_GRAPH_QUERY_LIMIT = 500;
const MAX_GRAPH_QUERY_LIMIT = 5000;

// #814: the precomputed snapshot covers the top-degree subgraph used by
// the empty-body / nodeType-only branch — the path the viewer hits on
// tab load. Sized to match the default query limit so the snapshot can
// service a default-cap request without falling back to live
// enumeration. Aggregate stats (nodesByType / edgesByType) are computed
// fresh during rebuild and stored alongside.
const SNAPSHOT_TOP_NODES = DEFAULT_GRAPH_QUERY_LIMIT;
const SNAPSHOT_KEY = "current";

// `state::list` over a 75K-node scope can exceed the iii invocation
// timeout. The query handler races the enumeration against this budget
// and falls back to the snapshot (or a warning envelope) when the live
// path is too slow. 6000ms leaves headroom under the default 8s engine
// invocation deadline.
const LIVE_ENUMERATION_BUDGET_MS = 6000;

function withTimeout<T>(p: Promise<T>, ms: number, label: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const t = setTimeout(
      () => reject(new Error(`${label}: exceeded ${ms}ms budget`)),
      ms,
    );
    p.then(
      (v) => {
        clearTimeout(t);
        resolve(v);
      },
      (err) => {
        clearTimeout(t);
        reject(err);
      },
    );
  });
}

function emptySnapshot(): GraphSnapshot {
  return {
    version: 1,
    topNodes: [],
    topEdges: [],
    topDegrees: {},
    stats: {
      totalNodes: 0,
      totalEdges: 0,
      nodesByType: {},
      edgesByType: {},
    },
    updatedAt: new Date(0).toISOString(),
    dirty: true,
  };
}

async function readSnapshot(kv: StateKV): Promise<GraphSnapshot | null> {
  try {
    const snap = await kv.get<GraphSnapshot>(KV.graphSnapshot, SNAPSHOT_KEY);
    if (snap && typeof snap === "object" && snap.version === 1) {
      return snap;
    }
    return null;
  } catch (err) {
    logger.warn("Graph snapshot read failed", {
      error: err instanceof Error ? err.message : String(err),
    });
    return null;
  }
}

function buildSnapshotFromArrays(
  nodes: GraphNode[],
  edges: GraphEdge[],
): GraphSnapshot {
  const liveNodes = nodes.filter((n) => !n.stale);
  const liveEdges = edges.filter((e) => !e.stale);
  // Build the global degree map once so we can both rank by it AND
  // snapshot the per-top-node values into topDegrees for synchronous
  // re-sort after incremental edge writes.
  const degree = new Map<string, number>();
  for (const e of liveEdges) {
    degree.set(e.sourceNodeId, (degree.get(e.sourceNodeId) ?? 0) + 1);
    degree.set(e.targetNodeId, (degree.get(e.targetNodeId) ?? 0) + 1);
  }
  const ranked = [...liveNodes]
    .sort((a, b) => (degree.get(b.id) ?? 0) - (degree.get(a.id) ?? 0))
    .slice(0, SNAPSHOT_TOP_NODES);
  const rankedIds = new Set(ranked.map((n) => n.id));
  const topEdges = liveEdges.filter(
    (e) => rankedIds.has(e.sourceNodeId) && rankedIds.has(e.targetNodeId),
  );
  const topDegrees: Record<string, number> = {};
  for (const n of ranked) {
    topDegrees[n.id] = degree.get(n.id) ?? 0;
  }
  const nodesByType: Record<string, number> = {};
  for (const n of liveNodes) {
    nodesByType[n.type] = (nodesByType[n.type] || 0) + 1;
  }
  const edgesByType: Record<string, number> = {};
  for (const e of liveEdges) {
    edgesByType[e.type] = (edgesByType[e.type] || 0) + 1;
  }
  return {
    version: 1,
    topNodes: ranked,
    topEdges,
    topDegrees,
    stats: {
      totalNodes: liveNodes.length,
      totalEdges: liveEdges.length,
      nodesByType,
      edgesByType,
    },
    updatedAt: new Date().toISOString(),
    dirty: false,
  };
}

function paginateFromSnapshot(
  snap: GraphSnapshot,
  filterType: string | undefined,
  limit: number,
  offset: number,
): GraphQueryResult {
  const filteredNodes = filterType
    ? snap.topNodes.filter((n) => n.type === filterType)
    : snap.topNodes;
  const total = filterType
    ? snap.stats.nodesByType[filterType] ?? 0
    : snap.stats.totalNodes;
  const pageNodes = filteredNodes.slice(offset, offset + limit);
  const pageIds = new Set(pageNodes.map((n) => n.id));
  const pageEdges = snap.topEdges.filter(
    (e) => pageIds.has(e.sourceNodeId) && pageIds.has(e.targetNodeId),
  );
  return {
    nodes: pageNodes,
    edges: pageEdges,
    depth: 0,
    totalNodes: total,
    totalEdges: snap.stats.totalEdges,
    truncated: total > pageNodes.length,
    limit,
    offset,
    fromSnapshot: true,
  };
}

// #814 v2: the rebuild path won't terminate on corpora large enough
// that kv.list returns a payload too big to JSON.parse without
// starving the iii heartbeat. We don't actually know the corpus size
// without enumerating, but we can refuse to start a rebuild if the
// snapshot's recorded `totalNodes` already exceeds this threshold —
// the rebuild path is unreliable above it, and an incremental
// extract-driven snapshot is the right approach for those corpora.
// Operators above the threshold should use mem::graph-reset and let
// future extracts rebuild incrementally.
const REBUILD_SAFE_NODE_CEILING = 25000;

function nameIndexKey(type: string, name: string): string {
  return `${type}|${name}`;
}

function edgeIndexKey(
  sourceNodeId: string,
  targetNodeId: string,
  type: string,
): string {
  return `${sourceNodeId}|${targetNodeId}|${type}`;
}

// Mutates `snap` to apply a +1 (or -1) degree delta for nodeId,
// maintaining the top-N ranking. Returns the new degree. Reads /
// writes the per-node degree counter via targeted kv.get/set so we
// never enumerate. Top-N membership flips when:
//   - node's new degree > current min in topNodes AND it's not in
//     topNodes (promote, evict tail if topNodes is full)
//   - node IS in topNodes and its position needs resorting (re-sort
//     topNodes in place)
async function applyDegreeDelta(
  kv: StateKV,
  snap: GraphSnapshot,
  nodeId: string,
  delta: number,
  // 批次内新建/合并的行还没落盘，优先从这个回调取（否则晋升 topNodes 时会
  // 读不到本批次刚建立的节点，与逐行写入的旧行为不一致）。
  lookup?: (nodeId: string) => GraphNode | undefined,
): Promise<number> {
  const prev = (await kv.get<number>(KV.graphNodeDegree, nodeId)) ?? 0;
  const next = Math.max(0, prev + delta);
  await kv.set(KV.graphNodeDegree, nodeId, next);

  const inTop = snap.topNodes.findIndex((n) => n.id === nodeId);
  if (inTop !== -1) {
    // Cache the new degree in topDegrees so the comparator runs
    // synchronously over numbers, not async kv.get calls. Re-sort
    // descending by degree.
    snap.topDegrees[nodeId] = next;
    snap.topNodes.sort(
      (a, b) =>
        (snap.topDegrees[b.id] ?? 0) - (snap.topDegrees[a.id] ?? 0),
    );
    return next;
  }

  const load = async (): Promise<GraphNode | null> =>
    lookup?.(nodeId) ?? (await kv.get<GraphNode>(KV.graphNodes, nodeId));

  if (snap.topNodes.length < SNAPSHOT_TOP_NODES) {
    // Capacity available — fetch + promote.
    const node = await load();
    if (node && !node.stale) {
      // 快照会随每次写入落盘，来源数组在这里就已经没有价值了 —— 而且它正是
      // 把 snapshot.bin 撑到 16.7MB 的原因。落快照前统一剥掉。
      const row = stripNodeObs(node);
      snap.topNodes.push(row);
      snap.topDegrees[node.id] = next;
      snap.topNodes.sort(
        (a, b) =>
          (snap.topDegrees[b.id] ?? 0) - (snap.topDegrees[a.id] ?? 0),
      );
    }
    return next;
  }

  // topNodes is full; the cutoff is the tail's cached degree.
  const tailEntry = snap.topNodes[snap.topNodes.length - 1];
  if (!tailEntry) return next;
  const tailDegree = snap.topDegrees[tailEntry.id] ?? 0;
  if (next > tailDegree) {
    const node = await load();
    if (node && !node.stale) {
      const row = stripNodeObs(node);
      const evicted = snap.topNodes.pop();
      if (evicted) delete snap.topDegrees[evicted.id];
      snap.topNodes.push(row);
      snap.topDegrees[node.id] = next;
      snap.topNodes.sort(
        (a, b) =>
          (snap.topDegrees[b.id] ?? 0) - (snap.topDegrees[a.id] ?? 0),
      );
    }
  }
  return next;
}

function snapshotPushEdgeIfBothInTop(
  snap: GraphSnapshot,
  edge: GraphEdge,
): void {
  const topIds = new Set(snap.topNodes.map((n) => n.id));
  if (topIds.has(edge.sourceNodeId) && topIds.has(edge.targetNodeId)) {
    // Dedupe in case the same edge gets pushed twice.
    if (!snap.topEdges.find((e) => e.id === edge.id)) {
      snap.topEdges.push(edge);
    }
  }
}

// 合并两个节点行。来源数组不在这里合并 —— 它已经外置到 mem:graph:node-obs，
// 由 persistGraphDelta 统一按 unionObs 累加后批量写回。这里只负责行内字段。
function mergeNode(
  existing: GraphNode,
  incoming: GraphNode,
  capturedAt: string,
): GraphNode {
  return {
    ...existing,
    properties: { ...existing.properties, ...incoming.properties },
    updatedAt: capturedAt,
  };
}

// 同上：来源数组的合并走旁路记录。
function mergeEdge(
  existing: GraphEdge,
  _obsIds: string[],
): GraphEdge {
  return { ...existing };
}

function resolvePagination(
  rawLimit: number | undefined,
  rawOffset: number | undefined,
): { limit: number; offset: number } {
  const requested = typeof rawLimit === "number" && Number.isFinite(rawLimit)
    ? Math.floor(rawLimit)
    : DEFAULT_GRAPH_QUERY_LIMIT;
  const limit = Math.max(1, Math.min(requested, MAX_GRAPH_QUERY_LIMIT));
  const offset = Math.max(
    0,
    typeof rawOffset === "number" && Number.isFinite(rawOffset)
      ? Math.floor(rawOffset)
      : 0,
  );
  return { limit, offset };
}

function paginate(
  nodes: GraphNode[],
  allEdges: GraphEdge[],
  depth: number,
  limit: number,
  offset: number,
): GraphQueryResult {
  const totalNodes = nodes.length;
  const pageNodes = nodes.slice(offset, offset + limit);
  const pageNodeIds = new Set(pageNodes.map((n) => n.id));
  // Edges restricted to the page so the response payload scales with
  // `limit`, not with the global edge count. An edge is included only
  // when BOTH endpoints land in the page — half-edges to nodes outside
  // the page would render as dangling links in the viewer.
  const pageEdges = allEdges.filter(
    (e) => pageNodeIds.has(e.sourceNodeId) && pageNodeIds.has(e.targetNodeId),
  );
  // Total edges (for the same node universe). Counted unbounded so the
  // viewer can show "showing X of Y" without re-querying.
  const universeIds = new Set(nodes.map((n) => n.id));
  const totalEdges = allEdges.reduce(
    (count, e) =>
      universeIds.has(e.sourceNodeId) && universeIds.has(e.targetNodeId)
        ? count + 1
        : count,
    0,
  );
  return {
    nodes: pageNodes,
    edges: pageEdges,
    depth,
    totalNodes,
    totalEdges,
    truncated: totalNodes > pageNodes.length,
    limit,
    offset,
  };
}

// 图谱查询结果的来源数组补全。
//
// 热行里的 sourceObservationIds 恒为空（真值在 mem:graph:node-obs /
// edge-obs），而 GraphQueryResult 是外部 API 的响应形状，调用方（图谱面板、
// MCP 资源、直接打 REST 的脚本）仍然期望看到来源链。这里只对「即将返回的
// 这一页」补全 —— 分页之后才 hydrate，成本与 limit 成正比，与图谱规模无关。
async function hydrateGraphQueryResult(
  kv: StateKV,
  result: GraphQueryResult,
): Promise<GraphQueryResult> {
  if (result.nodes.length === 0 && result.edges.length === 0) return result;
  const [nodes, edges] = await Promise.all([
    hydrateNodesObs(kv, result.nodes),
    hydrateEdgesObs(kv, result.edges),
  ]);
  return { ...result, nodes, edges };
}

// Parse all key="value" pairs from a tag's attribute string, in any
// order. The previous parser hard-coded attribute order
// (type before name on <entity>, type/source/target/weight on
// <relationship>) and silently dropped nodes/edges when the upstream
// LLM emitted attributes in a different order — Codex in particular
// likes to lead with `name=` (#635).
function parseAttrs(raw: string): Record<string, string> {
  const attrs: Record<string, string> = {};
  const attrRegex = /([A-Za-z_][\w:-]*)="([^"]*)"/g;
  let m;
  while ((m = attrRegex.exec(raw)) !== null) {
    attrs[m[1]] = m[2];
  }
  return attrs;
}

function parseGraphXml(
  xml: string,
  observationIds: string[],
): {
  nodes: GraphNode[];
  edges: GraphEdge[];
} {
  const nodes: GraphNode[] = [];
  const edges: GraphEdge[] = [];
  const now = new Date().toISOString();

  // Two passes because <entity> can be self-closing or have a body
  // (<property> children). The self-closing form needs `[^>]*[^/]` on
  // the attr group so the trailing `/` isn't swallowed into the match
  // (root cause of #494). The explicit-close form picks up the
  // property block.
  const entitySelfClose = /<entity\b([^>]*?)\/>/g;
  const entityWithBody = /<entity\b([^>]*[^/])>([\s\S]*?)<\/entity>/g;

  const addEntity = (rawAttrs: string, propsBlock = ""): void => {
    const attrs = parseAttrs(rawAttrs);
    const type = attrs["type"] as GraphNode["type"] | undefined;
    const name = attrs["name"];
    if (!type || !name) return;
    const properties: Record<string, string> = {};
    const propRegex = /<property\s+key="([^"]+)">([^<]*)<\/property>/g;
    let propMatch;
    while ((propMatch = propRegex.exec(propsBlock)) !== null) {
      properties[propMatch[1]] = propMatch[2];
    }
    nodes.push({
      id: generateId("gn"),
      type,
      name,
      properties,
      sourceObservationIds: observationIds,
      createdAt: now,
    });
  };

  let match;
  while ((match = entitySelfClose.exec(xml)) !== null) {
    addEntity(match[1]);
  }
  while ((match = entityWithBody.exec(xml)) !== null) {
    addEntity(match[1], match[2]);
  }

  const relRegex = /<relationship\b([^>]*?)\/>/g;
  while ((match = relRegex.exec(xml)) !== null) {
    const attrs = parseAttrs(match[1]);
    const type = attrs["type"] as GraphEdge["type"] | undefined;
    const sourceName = attrs["source"];
    const targetName = attrs["target"];
    if (!type || !sourceName || !targetName) continue;
    const parsedWeight = parseFloat(attrs["weight"] ?? "");
    const weight = Number.isFinite(parsedWeight) ? parsedWeight : 0.5;

    const sourceNode = nodes.find((n) => n.name === sourceName);
    const targetNode = nodes.find((n) => n.name === targetName);
    if (!sourceNode || !targetNode) continue;
    edges.push({
      id: generateId("ge"),
      type,
      sourceNodeId: sourceNode.id,
      targetNodeId: targetNode.id,
      weight: Math.max(0, Math.min(1, weight)),
      sourceObservationIds: observationIds,
      createdAt: now,
    });
  }

  return { nodes, edges };
}

const HEURISTIC_EDGE_WEIGHT = 0.4;
const MAX_HEURISTIC_EDGES_PER_OBS = 12;

export function extractGraphHeuristics(
  observations: CompressedObservation[],
): { nodes: GraphNode[]; edges: GraphEdge[] } {
  const now = new Date().toISOString();
  const nodes: GraphNode[] = [];
  const nodeByKey = new Map<string, GraphNode>();
  const edges: GraphEdge[] = [];
  const edgeByPair = new Map<string, GraphEdge>();

  const nodeFor = (
    type: GraphNode["type"],
    name: string,
    obsId: string,
  ): GraphNode | null => {
    const trimmed = name.trim();
    if (!trimmed) return null;
    const key = `${type} ${trimmed.toLowerCase()}`;
    let node = nodeByKey.get(key);
    if (!node) {
      node = {
        id: generateId("gn"),
        type,
        name: trimmed,
        properties: {},
        sourceObservationIds: [obsId],
        createdAt: now,
      };
      nodeByKey.set(key, node);
      nodes.push(node);
    } else if (!node.sourceObservationIds.includes(obsId)) {
      node.sourceObservationIds.push(obsId);
    }
    return node;
  };

  for (const obs of observations) {
    let budget = MAX_HEURISTIC_EDGES_PER_OBS;
    const link = (a: GraphNode | null, b: GraphNode | null): void => {
      if (!a || !b || a.id === b.id) return;
      const pair = a.id < b.id ? `${a.id}|${b.id}` : `${b.id}|${a.id}`;
      const existing = edgeByPair.get(pair);
      if (existing) {
        if (!existing.sourceObservationIds.includes(obs.id)) {
          existing.sourceObservationIds.push(obs.id);
        }
        return;
      }
      if (budget <= 0) return;
      budget -= 1;
      const edge: GraphEdge = {
        id: generateId("ge"),
        type: "related_to",
        sourceNodeId: a.id,
        targetNodeId: b.id,
        weight: HEURISTIC_EDGE_WEIGHT,
        sourceObservationIds: [obs.id],
        createdAt: now,
      };
      edgeByPair.set(pair, edge);
      edges.push(edge);
    };

    const fileNodes = (obs.files ?? []).map((f) =>
      nodeFor("file", f, obs.id),
    );
    const conceptNodes = (obs.concepts ?? []).map((c) =>
      nodeFor("concept", c, obs.id),
    );

    for (const concept of conceptNodes) {
      for (const file of fileNodes) link(concept, file);
    }
    for (let i = 0; i + 1 < conceptNodes.length; i++) {
      link(conceptNodes[i], conceptNodes[i + 1]);
    }
    for (let i = 0; i + 1 < fileNodes.length; i++) {
      link(fileNodes[i], fileNodes[i + 1]);
    }
  }

  return { nodes, edges };
}

// ---------------------------------------------------------------------------
// 有界「最早 K 个」收集器
// ---------------------------------------------------------------------------
// 回填 obs 反向索引时用来挑每个 obsId 最早的 OBS_INDEX_CAP 个节点。
//
// 之所以不用「全量收齐再排序」：实测每个 obsId 平均挂在 1198 个节点上，
// 4024 个 obsId 就是 482 万个 (obsId, nodeId) 对，全量攒在内存里约 290MB ——
// 而这个作业存在的意义恰恰是省内存。
//
// 之所以不用堆：满员之后绝大多数候选都该被直接拒绝，用「缓存当前最大值 +
// 一次字符串比较」就能挡掉。只有新候选确实更早时才回扫一次定位并替换，
// 随机顺序下回扫次数约为 K·ln(N/K)（K=64, N≈1200 时约 190 次/obsId），
// 相对 482 万次插入可以忽略。
class EarliestNodes {
  private readonly candidates: Array<{ id: string; at: string }> = [];
  private maxIndex = -1;

  constructor(private readonly limit: number) {}

  // 排序键：createdAt 升序，同一时刻按 nodeId 升序 —— 保证结果确定，
  // 重复回填得到同一份索引。
  private isLater(a: { id: string; at: string }, b: { id: string; at: string }): boolean {
    if (a.at !== b.at) return a.at > b.at;
    return a.id > b.id;
  }

  private recomputeMax(): void {
    let maxIndex = 0;
    for (let i = 1; i < this.candidates.length; i++) {
      if (this.isLater(this.candidates[i]!, this.candidates[maxIndex]!)) {
        maxIndex = i;
      }
    }
    this.maxIndex = maxIndex;
  }

  add(id: string, at: string): void {
    if (this.candidates.length < this.limit) {
      this.candidates.push({ id, at });
      if (this.candidates.length === this.limit) this.recomputeMax();
      return;
    }
    // 常见路径：候选不比当前最晚的一条更早，直接丢弃。
    if (!this.isLater({ id, at }, this.candidates[this.maxIndex]!)) return;
    this.candidates[this.maxIndex] = { id, at };
    this.recomputeMax();
  }

  ids(): string[] {
    return [...this.candidates]
      .sort((a, b) => (this.isLater(a, b) ? -1 : this.isLater(b, a) ? 1 : 0))
      .map((c) => c.id);
  }
}

// Shared persistence for a batch of extracted/imported nodes and edges.
// Factored out of mem::graph-extract so structural importers (graphify)
// reuse the exact same name-index upsert, degree bookkeeping, and snapshot
// maintenance — which also makes re-imports idempotent: an existing
// (type, name) resolves through the name index and merges instead of
// duplicating.
//
// #814 v2: targeted name-index lookups replace the O(n) scan over
// `kv.list<GraphNode>(KV.graphNodes)`. At 75K nodes the list payload
// exceeds the iii heartbeat budget and the worker dies before merge can
// complete. Each name-index entry is a single small kv.get/set pair.
//
// 来源数组外置之后（见 state/schema.ts），这个函数还承担了第二项职责：
// 把本批次产生的 obsId 累加进 mem:graph:node-obs / edge-obs 旁路记录，并
// 维护 obs 反向索引。为了让批量导入不至于退化成「每行两次 RPC」，解析阶段
// 全部走 getMany 批量读，写入阶段全部走 setMany 批量写。
export async function persistGraphDelta(
  kv: StateKV,
  nodes: GraphNode[],
  edges: GraphEdge[],
  obsIds: string[],
): Promise<{ newNodeCount: number; newEdgeCount: number }> {
  const snap = (await readSnapshot(kv)) ?? emptySnapshot();
  const capturedAt = new Date().toISOString();
  let newNodeCount = 0;
  let newEdgeCount = 0;
  // Merge-only batches mutate cached topNodes/topEdges entries without
  // changing the counts; track that separately so the snapshot still persists.
  let snapMutated = false;
  const newEdgesForTopCheck: GraphEdge[] = [];
  // When a freshly-minted node merges into an existing row via the name
  // index, edges in the same batch still reference the fresh id. Remap edge
  // endpoints to the persisted ids so edges never dangle and re-runs hit the
  // same edge-index key instead of duplicating.
  const idRemap = new Map<string, string>();

  // -------------------------------------------------------------------------
  // 节点：批量解析 → 内存合并 → 批量写回
  // -------------------------------------------------------------------------
  // 批次内可能出现同 (type,name) 的多个节点：原实现是顺序写、后一个通过名字
  // 索引读到前一个刚写入的行再合并。批量化的代价是「读」发生在全部「写」
  // 之前，所以要自己维护三张表把这件事还原回来：
  //   rowById      当前行内容（含批次内已发生的合并）
  //   freshByIndex 本批次新建的 indexKey -> nodeId
  //   obsById      累积的来源数组
  const nodeIndexKeys = nodes.map((n) => nameIndexKey(n.type, n.name));
  const nodeIdByIndexKey = await kv.getMany<string>(
    KV.graphNameIndex,
    nodeIndexKeys,
  );
  const candidateNodeIds = [...new Set(nodeIdByIndexKey.values())];
  const nodeRowsFromStore = await kv.getMany<GraphNode>(
    KV.graphNodes,
    candidateNodeIds,
  );

  const rowById = new Map<string, GraphNode>();
  const resolvedByIndexKey = new Map<string, GraphNode>();
  for (const [indexKey, nodeId] of nodeIdByIndexKey) {
    const row = nodeRowsFromStore.get(nodeId);
    if (!row) continue;
    // #825 follow-up: name-index lookups can resolve into pre-reset rows.
    // Drop them so extract writes a fresh node + index entry instead of
    // silently reconnecting to a legacy orphan (which would keep the
    // snapshot at 0 forever after a reset).
    if (
      snap.resetAt &&
      typeof row.createdAt === "string" &&
      row.createdAt < snap.resetAt
    ) {
      continue;
    }
    rowById.set(row.id, row);
    resolvedByIndexKey.set(indexKey, row);
  }

  const obsById = await readNodeObsMap(kv, [...rowById.values()]);
  const freshNodeIdByIndexKey = new Map<string, string>();
  const pendingNameIndexWrites: Array<readonly [string, string]> = [];
  const pendingDegreeWrites: Array<readonly [string, number]> = [];
  const touchedNodeIds: string[] = [];
  const seenTouchedNodeIds = new Set<string>();

  const accumulateNodeObs = (nodeId: string, incoming: readonly string[]): void => {
    obsById.set(nodeId, unionObs(obsById.get(nodeId), incoming, obsIds));
  };
  const markTouched = (nodeId: string): void => {
    if (seenTouchedNodeIds.has(nodeId)) return;
    seenTouchedNodeIds.add(nodeId);
    touchedNodeIds.push(nodeId);
  };

  for (const node of nodes) {
    const indexKey = nameIndexKey(node.type, node.name);
    const existing = resolvedByIndexKey.get(indexKey);

    if (existing) {
      idRemap.set(node.id, existing.id);
      const base = rowById.get(existing.id) ?? existing;
      const merged = stripNodeObs(mergeNode(base, node, capturedAt));
      rowById.set(existing.id, merged);
      accumulateNodeObs(existing.id, node.sourceObservationIds ?? []);
      markTouched(existing.id);
      // Update topNodes entry if present so a stale clone isn't returned
      // from the snapshot fast path.
      const topIdx = snap.topNodes.findIndex((n) => n.id === existing.id);
      if (topIdx !== -1) {
        snap.topNodes[topIdx] = merged;
        snapMutated = true;
      }
      continue;
    }

    const pendingId = freshNodeIdByIndexKey.get(indexKey);
    if (pendingId) {
      // 同批次内的重名节点：合并进本批次刚建的那一行。
      idRemap.set(node.id, pendingId);
      const base = rowById.get(pendingId)!;
      const merged = stripNodeObs(mergeNode(base, node, capturedAt));
      rowById.set(pendingId, merged);
      accumulateNodeObs(pendingId, node.sourceObservationIds ?? []);
      const topIdx = snap.topNodes.findIndex((n) => n.id === pendingId);
      if (topIdx !== -1) {
        snap.topNodes[topIdx] = merged;
        snapMutated = true;
      }
      continue;
    }

    const fresh = stripNodeObs(node);
    rowById.set(node.id, fresh);
    freshNodeIdByIndexKey.set(indexKey, node.id);
    pendingNameIndexWrites.push([indexKey, node.id]);
    pendingDegreeWrites.push([node.id, 0]);
    accumulateNodeObs(node.id, node.sourceObservationIds ?? []);
    markTouched(node.id);
    snap.stats.totalNodes += 1;
    snap.stats.nodesByType[node.type] =
      (snap.stats.nodesByType[node.type] ?? 0) + 1;
    newNodeCount += 1;
    if (snap.topNodes.length < SNAPSHOT_TOP_NODES) {
      // Degree 0 still beats an empty slot — sit at the tail until edges
      // arrive and promote.
      snap.topNodes.push(fresh);
      snap.topDegrees[node.id] = 0;
    }
  }

  // -------------------------------------------------------------------------
  // 边：同样的批量解析 → 合并 → 写回
  // -------------------------------------------------------------------------
  const remappedEdges: GraphEdge[] = edges.map((raw) => ({
    ...raw,
    sourceNodeId: idRemap.get(raw.sourceNodeId) ?? raw.sourceNodeId,
    targetNodeId: idRemap.get(raw.targetNodeId) ?? raw.targetNodeId,
  }));
  const edgeIndexKeys = remappedEdges.map((e) =>
    edgeIndexKey(e.sourceNodeId, e.targetNodeId, e.type),
  );
  const edgeIdByIndexKey = await kv.getMany<string>(
    KV.graphEdgeKey,
    edgeIndexKeys,
  );
  const edgeRowsFromStore = await kv.getMany<GraphEdge>(
    KV.graphEdges,
    [...new Set(edgeIdByIndexKey.values())],
  );

  const edgeRowById = new Map<string, GraphEdge>();
  const resolvedEdgeByIndexKey = new Map<string, GraphEdge>();
  for (const [indexKey, edgeId] of edgeIdByIndexKey) {
    const row = edgeRowsFromStore.get(edgeId);
    if (!row) continue;
    // Same #825 orphan check as the node path above.
    if (
      snap.resetAt &&
      typeof row.createdAt === "string" &&
      row.createdAt < snap.resetAt
    ) {
      continue;
    }
    edgeRowById.set(row.id, row);
    resolvedEdgeByIndexKey.set(indexKey, row);
  }

  const edgeObsById = await readEdgeObsMap(kv, [...edgeRowById.values()]);
  const freshEdgeIdByIndexKey = new Map<string, string>();
  const pendingEdgeIndexWrites: Array<readonly [string, string]> = [];
  const touchedEdgeIds: string[] = [];
  const seenTouchedEdgeIds = new Set<string>();

  const accumulateEdgeObs = (edgeId: string): void => {
    edgeObsById.set(edgeId, unionObs(edgeObsById.get(edgeId), obsIds));
  };
  const markEdgeTouched = (edgeId: string): void => {
    if (seenTouchedEdgeIds.has(edgeId)) return;
    seenTouchedEdgeIds.add(edgeId);
    touchedEdgeIds.push(edgeId);
  };

  for (let i = 0; i < remappedEdges.length; i++) {
    const edge = remappedEdges[i]!;
    const eKey = edgeIndexKeys[i]!;
    const existing = resolvedEdgeByIndexKey.get(eKey);

    if (existing) {
      const base = edgeRowById.get(existing.id) ?? existing;
      const merged = stripEdgeObs(mergeEdge(base, obsIds));
      edgeRowById.set(existing.id, merged);
      accumulateEdgeObs(existing.id);
      markEdgeTouched(existing.id);
      // Replace cached topEdges entry too if present.
      const topIdx = snap.topEdges.findIndex((e) => e.id === existing.id);
      if (topIdx !== -1) {
        snap.topEdges[topIdx] = merged;
        snapMutated = true;
      }
      continue;
    }

    const pendingId = freshEdgeIdByIndexKey.get(eKey);
    if (pendingId) {
      const base = edgeRowById.get(pendingId)!;
      const merged = stripEdgeObs(mergeEdge(base, obsIds));
      edgeRowById.set(pendingId, merged);
      accumulateEdgeObs(pendingId);
      const topIdx = snap.topEdges.findIndex((e) => e.id === pendingId);
      if (topIdx !== -1) {
        snap.topEdges[topIdx] = merged;
        snapMutated = true;
      }
      continue;
    }

    const fresh = stripEdgeObs(edge);
    edgeRowById.set(edge.id, fresh);
    freshEdgeIdByIndexKey.set(eKey, edge.id);
    pendingEdgeIndexWrites.push([eKey, edge.id]);
    accumulateEdgeObs(edge.id);
    markEdgeTouched(edge.id);
    snap.stats.totalEdges += 1;
    snap.stats.edgesByType[edge.type] =
      (snap.stats.edgesByType[edge.type] ?? 0) + 1;
    newEdgeCount += 1;
    await applyDegreeDelta(kv, snap, edge.sourceNodeId, +1, (id) =>
      rowById.get(id),
    );
    await applyDegreeDelta(kv, snap, edge.targetNodeId, +1, (id) =>
      rowById.get(id),
    );
    newEdgesForTopCheck.push(fresh);
  }

  // Push newly-added edges into snapshot.topEdges if both endpoints are in
  // the top-N (post-degree-delta). Done after all degree updates so the
  // topIds set is stable.
  for (const edge of newEdgesForTopCheck) {
    snapshotPushEdgeIfBothInTop(snap, edge);
  }

  // -------------------------------------------------------------------------
  // 落盘。顺序是有意为之：来源数组必须早于热行写入。
  // -------------------------------------------------------------------------
  // 热行里 sourceObservationIds 恒为空，真值只在旁路记录里。如果先写热行、
  // 中途失败，这一批的来源数据就永久丢了。反过来先写旁路、后写热行，最坏
  // 情况只是旁路记录里多了一批「热行尚未置空」的数据，重跑即可自愈。
  if (touchedNodeIds.length > 0) {
    await writeNodeObsMany(
      kv,
      touchedNodeIds.map((id) => [id, obsById.get(id) ?? []] as const),
    );
  }
  if (touchedEdgeIds.length > 0) {
    await writeEdgeObsMany(
      kv,
      touchedEdgeIds.map((id) => [id, edgeObsById.get(id) ?? []] as const),
    );
  }
  if (touchedNodeIds.length > 0 && obsIds.length > 0) {
    await indexObsToNodes(kv, touchedNodeIds, obsIds);
  }
  if (touchedNodeIds.length > 0) {
    await kv.setMany(
      KV.graphNodes,
      touchedNodeIds.map((id) => [id, rowById.get(id)!] as const),
    );
  }
  if (touchedEdgeIds.length > 0) {
    await kv.setMany(
      KV.graphEdges,
      touchedEdgeIds.map((id) => [id, edgeRowById.get(id)!] as const),
    );
  }
  if (pendingNameIndexWrites.length > 0) {
    await kv.setMany(KV.graphNameIndex, pendingNameIndexWrites);
  }
  if (pendingDegreeWrites.length > 0) {
    await kv.setMany(KV.graphNodeDegree, pendingDegreeWrites);
  }
  if (pendingEdgeIndexWrites.length > 0) {
    await kv.setMany(KV.graphEdgeKey, pendingEdgeIndexWrites);
  }

  if (newNodeCount > 0 || newEdgeCount > 0 || snapMutated) {
    snap.updatedAt = capturedAt;
    snap.dirty = false;
    await kv.set(KV.graphSnapshot, SNAPSHOT_KEY, snap);
  }

  return { newNodeCount, newEdgeCount };
}

export function registerGraphFunction(
  sdk: ISdk,
  kv: StateKV,
  provider: MemoryProvider,
): void {
  sdk.registerFunction("mem::graph-extract",
    async (data: { observations: CompressedObservation[] }) => {
      if (!data.observations || data.observations.length === 0) {
        return { success: false, error: "No observations provided" };
      }

      const obsIds = data.observations.map((o) => o.id);

      let nodes: GraphNode[] = [];
      let edges: GraphEdge[] = [];
      try {
        const heuristic = extractGraphHeuristics(data.observations);
        nodes = heuristic.nodes;
        edges = heuristic.edges;
      } catch (err) {
        logger.warn("heuristic graph extraction failed", {
          error: err instanceof Error ? err.message : String(err),
        });
      }

      const llmEnabled =
        isGraphExtractionEnabled() && !provider.name.includes("noop");
      let llmError: string | undefined;
      if (llmEnabled) {
        const prompt = buildGraphExtractionPrompt(
          data.observations.map((o) => ({
            title: o.title,
            narrative: o.narrative,
            concepts: o.concepts,
            files: o.files,
            type: o.type,
          })),
        );
        try {
          const response = await provider.compress(
            GRAPH_EXTRACTION_SYSTEM,
            prompt,
          );
          const parsed = parseGraphXml(response, obsIds);
          nodes = nodes.concat(parsed.nodes);
          edges = edges.concat(parsed.edges);
        } catch (err) {
          llmError = err instanceof Error ? err.message : String(err);
          logger.error("LLM graph extraction failed", { error: llmError });
        }
      }

      if (nodes.length === 0 && edges.length === 0) {
        return llmError
          ? { success: false, error: llmError }
          : { success: true, nodesAdded: 0, edgesAdded: 0 };
      }

      try {
        const { newNodeCount, newEdgeCount } = await persistGraphDelta(
          kv,
          nodes,
          edges,
          obsIds,
        );

        await recordAudit(kv, "observe", "mem::graph-extract", obsIds, {
          nodesExtracted: nodes.length,
          edgesExtracted: edges.length,
        });

        logger.info("Graph extraction complete", {
          nodes: nodes.length,
          edges: edges.length,
          newNodes: newNodeCount,
          newEdges: newEdgeCount,
          llm: llmEnabled && !llmError,
        });
        return {
          success: true,
          nodesAdded: nodes.length,
          edgesAdded: edges.length,
        };
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger.error("Graph extraction failed", { error: msg });
        return { success: false, error: msg };
      }
    },
  );

  // #753: every branch now applies a default cap and reports the
  // unbounded `total*` counts. Before this change, an unfiltered POST
  // /graph/query body (`{}`) on a corpus with ~10k+ nodes serialized
  // to a payload large enough that the iii state response channel
  // rejected it with HTTP 500 "Invocation stopped", leaving the viewer
  // graph tab silently blank.
  sdk.registerFunction("mem::graph-query",
    async (data: {
      startNodeId?: string;
      nodeType?: string;
      maxDepth?: number;
      query?: string;
      limit?: number;
      offset?: number;
    }): Promise<GraphQueryResult> => {
      const maxDepth = Math.min(data.maxDepth || 3, 5);
      const { limit, offset } = resolvePagination(data.limit, data.offset);

      // #814 v2: the empty-body / nodeType-only path NEVER enumerates.
      // It reads the snapshot exclusively. The snapshot is updated
      // inline by graph-extract, so for newly-built corpora it's
      // always current. For legacy corpora missing a snapshot the
      // operator must run mem::graph-snapshot-rebuild (safe under
      // REBUILD_SAFE_NODE_CEILING) or mem::graph-reset to wipe and
      // rebuild incrementally from new observations.
      const noWalk = !data.query && !data.startNodeId;
      if (noWalk) {
        const snap = await readSnapshot(kv);
        if (snap && snap.stats.totalNodes > 0) {
          return paginateFromSnapshot(snap, data.nodeType, limit, offset);
        }
        return {
          nodes: [],
          edges: [],
          depth: 0,
          totalNodes: 0,
          totalEdges: 0,
          truncated: false,
          limit,
          offset,
          warning:
            "No graph snapshot available. Either no graph has been " +
            "extracted yet, or you are on a legacy corpus from a pre-#814 " +
            "agentmemory build. Run POST /agentmemory/graph/snapshot-rebuild " +
            "(safe up to ~25K nodes) or POST /agentmemory/graph/reset to " +
            "wipe and let future extracts repopulate.",
        };
      }

      // Query / startNodeId paths still need broader access, so they list
      // the node/edge scopes. That path used to be the dangerous one: at
      // 18.6K nodes / 33.2K edges the two payloads were 132.8MB + 230MB of
      // JSON, and parsing them starved the iii heartbeat (~0.35s) long
      // before any Promise.race timer could fire — the caller saw
      // "Invocation stopped" and the worker was restarted.
      //
      // Since the provenance arrays moved out of the hot rows (see
      // state/schema.ts) the same two scopes hold 4.0MB + 7.7MB, so a plain
      // enumeration is cheap. The wall-clock race stays as a regression
      // guard: if a future change re-inflates the rows, callers still get a
      // snapshot-backed answer instead of a 500.
      let allNodes: GraphNode[];
      let allEdges: GraphEdge[];
      try {
        const [rawNodes, rawEdges] = await withTimeout(
          Promise.all([
            kv.list<GraphNode>(KV.graphNodes),
            kv.list<GraphEdge>(KV.graphEdges),
          ]),
          LIVE_ENUMERATION_BUDGET_MS,
          "graph-query enumeration",
        );
        allNodes = rawNodes.filter((n) => !n.stale);
        allEdges = rawEdges.filter((e) => !e.stale);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger.warn("Graph query enumeration timed out, using snapshot", {
          error: msg,
        });
        const snap = await readSnapshot(kv);
        if (snap) {
          return {
            ...paginateFromSnapshot(snap, data.nodeType, limit, offset),
            warning:
              "Live graph enumeration exceeded its budget; the result " +
              "reflects the top-degree snapshot rather than the requested " +
              "walk. This points at an over-sized node/edge row — check " +
              "that sourceObservationIds is not being written back into " +
              "mem:graph:nodes / mem:graph:edges.",
          };
        }
        return {
          nodes: [],
          edges: [],
          depth: 0,
          totalNodes: 0,
          totalEdges: 0,
          truncated: false,
          limit,
          offset,
          warning:
            "Graph enumeration exceeded budget and no snapshot is available.",
        };
      }

      if (data.query) {
        const lower = data.query.toLowerCase();
        const matchingNodes = allNodes.filter(
          (n) =>
            n.name.toLowerCase().includes(lower) ||
            Object.values(n.properties).some(
              (v) => typeof v === "string" && v.toLowerCase().includes(lower),
            ),
        );
        return hydrateGraphQueryResult(
          kv,
          paginate(matchingNodes, allEdges, 0, limit, offset),
        );
      }

      if (data.startNodeId) {
        // 邻接表在遍历前一次性建好。
        //
        // 原实现对每个访问到的节点都跑一遍 allEdges.filter，整体是 O(V×E)：
        // 500 个节点 × 33K 条边 = 1650 万次比较，而且 allNodes.find 同样是
        // 每个节点一次线性扫描。建表之后是 O(E) + O(Σdeg)，稀疏图上快了一到
        // 两个数量级 —— 这部分是纯 CPU，但同样占用事件循环，会挤压心跳预算。
        const nodeById = new Map<string, GraphNode>();
        for (const node of allNodes) nodeById.set(node.id, node);
        const adjacency = new Map<string, GraphEdge[]>();
        for (const edge of allEdges) {
          const source = adjacency.get(edge.sourceNodeId);
          if (source) source.push(edge);
          else adjacency.set(edge.sourceNodeId, [edge]);
          const target = adjacency.get(edge.targetNodeId);
          if (target) target.push(edge);
          else adjacency.set(edge.targetNodeId, [edge]);
        }

        const visited = new Set<string>();
        const visitedEdges = new Set<string>();
        const resultNodes: GraphNode[] = [];
        const resultEdges: GraphEdge[] = [];
        const queue: Array<{ nodeId: string; depth: number }> = [
          { nodeId: data.startNodeId, depth: 0 },
        ];

        while (queue.length > 0) {
          const { nodeId, depth } = queue.shift()!;
          if (visited.has(nodeId) || depth > maxDepth) continue;
          visited.add(nodeId);

          const node = nodeById.get(nodeId);
          if (node) {
            if (!data.nodeType || node.type === data.nodeType) {
              resultNodes.push(node);
            }
          }

          for (const edge of adjacency.get(nodeId) ?? []) {
            if (!visitedEdges.has(edge.id)) {
              visitedEdges.add(edge.id);
              resultEdges.push(edge);
            }
            const nextId =
              edge.sourceNodeId === nodeId
                ? edge.targetNodeId
                : edge.sourceNodeId;
            if (!visited.has(nextId)) {
              queue.push({ nodeId: nextId, depth: depth + 1 });
            }
          }
        }

        return hydrateGraphQueryResult(
          kv,
          paginate(resultNodes, resultEdges, maxDepth, limit, offset),
        );
      }

      // Unreachable — noWalk branch handles the rest.
      return hydrateGraphQueryResult(
        kv,
        paginate([], [], 0, limit, offset),
      );
    },
  );

  // #814 v2: graph-stats reads the snapshot exclusively. The snapshot
  // is maintained inline by mem::graph-extract, so for any corpus built
  // on a post-#814 agentmemory the stats are always current without an
  // enumeration. Legacy corpora without a snapshot get an empty
  // envelope + a warning pointing at the snapshot-rebuild or graph-reset
  // endpoints — never a 500.
  sdk.registerFunction("mem::graph-stats", async () => {
    const snap = await readSnapshot(kv);
    if (snap) {
      return {
        ...snap.stats,
        fromSnapshot: true,
        updatedAt: snap.updatedAt,
        ...(snap.dirty
          ? {
              warning:
                "Snapshot is marked dirty (write was in-flight when read). " +
                "Counts are eventually consistent.",
            }
          : {}),
      };
    }
    return {
      totalNodes: 0,
      totalEdges: 0,
      nodesByType: {},
      edgesByType: {},
      fromSnapshot: false,
      warning:
        "No graph snapshot available. Run POST /agentmemory/graph/snapshot-rebuild " +
        "(safe up to ~25K nodes) or POST /agentmemory/graph/reset to wipe " +
        "and let future extracts repopulate.",
    };
  });

  // #814 v2: explicit rebuild backfills the snapshot AND the name /
  // edge-key / degree indexes from existing graphNodes/graphEdges
  // scopes. This is the path operators run once after upgrading to a
  // post-#814 build to bring legacy corpora online. It enumerates via
  // kv.list — the same pair that breaks at 75K+ — so we refuse to
  // run on corpora large enough that the response payload would
  // block the worker heartbeat. Above the ceiling the only safe path
  // is mem::graph-reset followed by incremental re-extraction.
  sdk.registerFunction(
    "mem::graph-snapshot-rebuild",
    async (data?: { force?: boolean }) => {
      const started = Date.now();
      // #825: pre-flight refusal for legacy corpora. The old guard
      // checked node count AFTER kv.list, but the heartbeat dies at
      // ~0.35s on a 75K-node response — long before the wall-clock
      // budget can fire. We can't safely enumerate to discover size.
      //
      // Heuristic: if no snapshot exists, the corpus is either empty
      // or legacy. The empty case has nothing to rebuild; the legacy
      // case will crash. Refuse both unless `force: true` is passed
      // (operator opt-in to attempt rebuild on a corpus they know is
      // small enough — typically under 10K nodes on the default iii
      // state adapter).
      // Strict boolean check on force — accept only literal `true`,
      // never truthy strings/numbers, so a hand-crafted JSON payload
      // can't accidentally bypass the legacy-corpus safeguard.
      const forceRebuild = data?.force === true;
      try {
        const existing = await readSnapshot(kv);
        if (!existing && !forceRebuild) {
          logger.warn("Graph snapshot rebuild refused: no prior snapshot", {
            hint: "legacy corpus or empty store",
          });
          return {
            success: false,
            legacyCorpus: true,
            error:
              "No prior snapshot found. Rebuild would call kv.list on " +
              "KV.graphNodes/Edges, which heartbeat-crashes the worker " +
              "on corpora past the iii state response budget (~25K nodes). " +
              "Either (a) call POST /agentmemory/graph/reset to drop into " +
              "incremental-only mode and rebuild from new extracts, or " +
              "(b) re-send with `force: true` if you're certain the " +
              "corpus is small.",
          };
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger.warn("Graph snapshot pre-flight read failed", { error: msg });
        // Fall through; the user passed force=true or the snapshot
        // read itself failed (separate problem).
      }

      try {
        const [nodes, edges] = await withTimeout(
          Promise.all([
            kv.list<GraphNode>(KV.graphNodes),
            kv.list<GraphEdge>(KV.graphEdges),
          ]),
          LIVE_ENUMERATION_BUDGET_MS,
          "graph-snapshot-rebuild enumeration",
        );

      if (nodes.length > REBUILD_SAFE_NODE_CEILING) {
        logger.warn("Graph snapshot rebuild aborted: corpus too large", {
          totalNodes: nodes.length,
          ceiling: REBUILD_SAFE_NODE_CEILING,
        });
        return {
          success: false,
          tooLarge: true,
          totalNodes: nodes.length,
          ceiling: REBUILD_SAFE_NODE_CEILING,
          error:
            `Corpus has ${nodes.length} graph nodes; safe-rebuild ceiling ` +
            `is ${REBUILD_SAFE_NODE_CEILING}. Run POST /agentmemory/graph/reset ` +
            `to wipe and let future extracts rebuild incrementally.`,
        };
      }

      // Backfill the targeted-lookup indexes so post-rebuild
      // graph-extract calls hit the O(1) path instead of falling
      // through to the (already-removed) full-scope scan. Batch
      // writes via Promise.all to avoid N sequential round-trips —
      // BATCH_SIZE bounds in-flight writes so we don't open thousands
      // of concurrent state channels on huge corpora.
      const liveNodes = nodes.filter((n) => !n.stale);
      const liveEdges = edges.filter((e) => !e.stale);
      const degree = new Map<string, number>();
      for (const e of liveEdges) {
        degree.set(e.sourceNodeId, (degree.get(e.sourceNodeId) ?? 0) + 1);
        degree.set(e.targetNodeId, (degree.get(e.targetNodeId) ?? 0) + 1);
      }
      const BATCH_SIZE = 100;
      for (let i = 0; i < liveNodes.length; i += BATCH_SIZE) {
        const batch = liveNodes.slice(i, i + BATCH_SIZE);
        await Promise.all(
          batch.flatMap((n) => [
            kv.set(KV.graphNameIndex, nameIndexKey(n.type, n.name), n.id),
            kv.set(KV.graphNodeDegree, n.id, degree.get(n.id) ?? 0),
          ]),
        );
      }
      for (let i = 0; i < liveEdges.length; i += BATCH_SIZE) {
        const batch = liveEdges.slice(i, i + BATCH_SIZE);
        await Promise.all(
          batch.map((e) =>
            kv.set(
              KV.graphEdgeKey,
              edgeIndexKey(e.sourceNodeId, e.targetNodeId, e.type),
              e.id,
            ),
          ),
        );
      }

      const snap = buildSnapshotFromArrays(nodes, edges);
      await kv.set(KV.graphSnapshot, SNAPSHOT_KEY, snap);
      const tookMs = Date.now() - started;
      logger.info("Graph snapshot rebuilt", {
        totalNodes: snap.stats.totalNodes,
        totalEdges: snap.stats.totalEdges,
        topNodes: snap.topNodes.length,
        topEdges: snap.topEdges.length,
        tookMs,
      });
      return {
        success: true,
        ...snap.stats,
        topNodes: snap.topNodes.length,
        topEdges: snap.topEdges.length,
        updatedAt: snap.updatedAt,
        tookMs,
      };
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.error("Graph snapshot rebuild failed", { error: msg });
      return { success: false, error: msg };
    }
  });

  // -------------------------------------------------------------------------
  // mem::graph-provenance-backfill
  // -------------------------------------------------------------------------
  // 一次性维护作业：把存量数据的来源数组从热行搬到旁路 scope。
  //
  // 这次修复本身是向后兼容的（旁路缺失时读取回退到热行内旧值），所以存量
  // 数据在新代码下能正常工作 —— 但热行依然是 132.8MB + 230MB，枚举照样会
  // 打挂心跳。要真正吃到修复，存量语料必须跑一遍这个函数把来源数组搬走。
  //
  // 实现刻意走 kv.rewriteScope（按分片重写）而不是 kv.list + 逐行 set：
  //   * 峰值内存只有一个分片几十 KB，而不是整个 scope；
  //   * 每个分片只做一次读改写，避免「写 N 行 = 重写 N 次分片文件」的放大。
  //
  // 幂等：来源数组已经是空数组的行原样返回，重复执行只是空转一遍。
  sdk.registerFunction("mem::graph-provenance-backfill",
    async (data?: { skipObsIndex?: boolean }) => {
      const started = Date.now();
      const counters = {
        nodesScanned: 0,
        nodeObsWritten: 0,
        edgesScanned: 0,
        edgeObsWritten: 0,
      };

      // 反向索引的截断规则是「按 createdAt 取最早的 OBS_INDEX_CAP 个节点」，
      // 而分片遍历顺序与时间无关，所以只能先把候选收齐、最后一次性定序写入。
      const obsCandidates = new Map<string, EarliestNodes>();

      const nodeStats = await kv.rewriteScope(
        KV.graphNodes,
        async (rows) => {
          const obsEntries: Array<readonly [string, readonly string[]]> = [];
          const next: Record<string, unknown> = {};
          for (const [id, raw] of Object.entries(rows)) {
            const node = raw as GraphNode;
            const obs = node.sourceObservationIds ?? [];
            if (obs.length > 0) {
              obsEntries.push([id, obs]);
              const at = typeof node.createdAt === "string" ? node.createdAt : "";
              for (const obsId of obs) {
                let bucket = obsCandidates.get(obsId);
                if (!bucket) {
                  bucket = new EarliestNodes(OBS_INDEX_CAP);
                  obsCandidates.set(obsId, bucket);
                }
                bucket.add(id, at);
              }
            }
            next[id] = obs.length > 0 ? stripNodeObs(node) : node;
            counters.nodesScanned += 1;
          }
          // 先写旁路记录，再由 rewriteScope 写回热行 —— 顺序与在线写入一致，
          // 中途失败也不会丢来源数据。
          if (obsEntries.length > 0) {
            await writeNodeObsMany(kv, obsEntries);
            counters.nodeObsWritten += obsEntries.length;
          }
          return next;
        },
        { concurrency: 4 },
      );

      const edgeStats = await kv.rewriteScope(
        KV.graphEdges,
        async (rows) => {
          const obsEntries: Array<readonly [string, readonly string[]]> = [];
          const next: Record<string, unknown> = {};
          for (const [id, raw] of Object.entries(rows)) {
            const edge = raw as GraphEdge;
            const obs = edge.sourceObservationIds ?? [];
            if (obs.length > 0) obsEntries.push([id, obs]);
            next[id] = obs.length > 0 ? stripEdgeObs(edge) : edge;
            counters.edgesScanned += 1;
          }
          if (obsEntries.length > 0) {
            await writeEdgeObsMany(kv, obsEntries);
            counters.edgeObsWritten += obsEntries.length;
          }
          return next;
        },
        { concurrency: 4 },
      );

      let obsIndexEntries = 0;
      if (!data?.skipObsIndex) {
        const entries: Array<readonly [string, string[]]> = [];
        for (const [obsId, bucket] of obsCandidates) {
          const ids = bucket.ids();
          if (ids.length > 0) entries.push([obsId, ids]);
        }
        if (entries.length > 0) await kv.setMany(KV.graphObsIndex, entries);
        obsIndexEntries = entries.length;
      }

      // 快照里同样带着来源数组（snapshot.bin 实测 16.7MB），一并剥掉。
      // 不处理的话 graph/query 的 no-walk 快路径仍然会把十几 MB 读进内存。
      let snapshotStripped = 0;
      const snap = await readSnapshot(kv);
      if (snap) {
        for (let i = 0; i < snap.topNodes.length; i++) {
          const node = snap.topNodes[i]!;
          if ((node.sourceObservationIds ?? []).length > 0) {
            snap.topNodes[i] = stripNodeObs(node);
            snapshotStripped += 1;
          }
        }
        for (let i = 0; i < snap.topEdges.length; i++) {
          const edge = snap.topEdges[i]!;
          if ((edge.sourceObservationIds ?? []).length > 0) {
            snap.topEdges[i] = stripEdgeObs(edge);
            snapshotStripped += 1;
          }
        }
        if (snapshotStripped > 0) {
          await kv.set(KV.graphSnapshot, SNAPSHOT_KEY, snap);
        }
      }

      const tookMs = Date.now() - started;
      logger.info("Graph provenance backfill complete", {
        ...counters,
        nodeShards: nodeStats.shards,
        edgeShards: edgeStats.shards,
        obsIndexEntries,
        snapshotStripped,
        tookMs,
      });
      return {
        success: true,
        ...counters,
        nodeShards: nodeStats.shards,
        edgeShards: edgeStats.shards,
        obsIndexEntries,
        snapshotStripped,
        tookMs,
      };
    },
  );

  // #814 v2 + #825: clean-restart escape hatch for corpora of any
  // size, including the legacy 75K+ case that crashes kv.list.
  //
  // Previous reset walked kv.list<GraphNode/Edge>(...) which is the
  // exact primitive that heartbeat-crashes the worker on the corpus
  // this reset was meant to recover (Allan's repro, 0.35s death).
  //
  // The new design is enumeration-free: write an empty snapshot and
  // return. The hot path (mem::graph-query empty-body, mem::graph-stats)
  // reads ONLY the snapshot post-#816, so a fresh empty snapshot
  // makes the graph behave as if it were empty for every read.
  //
  // Future extracts repopulate the snapshot + side-indexes
  // incrementally (graph-extract is O(1) per node post-#816 — it does
  // not consult the legacy rows).
  //
  // Trade-off: legacy rows in KV.graphNodes / KV.graphEdges remain on
  // disk as unreferenced orphans. They consume disk but are never
  // read by any post-#816 code path. Cleanup is deferred to a future
  // chunked-vacuum job; #816's broken vacuum-via-list strategy is
  // what we are leaving behind here.
  sdk.registerFunction("mem::graph-reset", async () => {
    const started = Date.now();
    // Stamp resetAt=now on the empty snapshot. Future
    // mem::graph-extract calls compare each name-index lookup's
    // existing node `createdAt` against this timestamp; anything
    // older counts as an orphan and is dropped from the merge path,
    // forcing extract to write a fresh row instead of reconnecting
    // to a pre-reset entry.
    const resetSnapshot: GraphSnapshot = {
      ...emptySnapshot(),
      resetAt: new Date().toISOString(),
    };
    await kv.set(KV.graphSnapshot, SNAPSHOT_KEY, resetSnapshot);
    const counts: Record<string, number> = {
      [KV.graphSnapshot]: 1,
    };
    const tookMs = Date.now() - started;
    logger.info("Graph state reset", { counts, tookMs });
    return { success: true, cleared: counts, tookMs };
  });
}
