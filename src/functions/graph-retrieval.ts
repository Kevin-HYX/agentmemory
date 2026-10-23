import type {
  GraphNode,
  GraphEdge,
} from "../types.js";
import { KV } from "../state/schema.js";
import type { StateKV } from "../state/kv.js";
import {
  findNodeIdsByObsIds,
  hydrateEdgesObs,
  hydrateNodesObs,
  readNodeObsMap,
} from "../state/graph-provenance.js";

export interface GraphRetrievalResult {
  obsId: string;
  sessionId: string;
  score: number;
  graphContext: string;
  pathLength: number;
}

// 一次检索里按需补全来源数组的批量大小。
//
// 这里原本是一个固定的补全总量上限（MAX_OBSID_HYDRATIONS = 400），用来避免
// 在深度 2、可达集上千时逐个补来源。但它有一个致命副作用：补全与否决定了一个
// 节点能不能发射观测，而 obsId 的归属权是先到先得的 —— 于是「补全到第几个节点」
// 这个纯 I/O 细节泄漏进了结果集，同一查询在预算边界两侧会给出不同的 top-N
// （实测 8 个真实查询词全部不同）。批量大小不改变结果，只是在「多读几次」和
// 「一次读太多」之间取舍，因此这里退回成纯批量参数。
const OBS_HYDRATION_BATCH = 128;

type Path = Array<{ node: GraphNode; edge?: GraphEdge }>;

// 图谱索引在单次检索内只建一次。
//
// 原实现把它建在 dijkstraTraversal 里，而那个函数是按起点逐个调用的 ——
// 匹配到 500 个起点就要把 33K 条边、18K 个节点重建 500 遍，单次检索的
// CPU 开销直接到亿级。这里提到方法入口建一次，复杂度从 O(起点数 × (V+E))
// 降到 O(V+E)。
interface GraphIndex {
  nodeById: Map<string, GraphNode>;
  adjacency: Map<string, Array<{ neighborId: string; edge: GraphEdge }>>;
}

function buildGraphIndex(
  nodes: readonly GraphNode[],
  edges: readonly GraphEdge[],
): GraphIndex {
  const nodeById = new Map<string, GraphNode>();
  for (const node of nodes) nodeById.set(node.id, node);
  const adjacency = new Map<string, Array<{ neighborId: string; edge: GraphEdge }>>();
  for (const edge of edges) {
    const source = adjacency.get(edge.sourceNodeId);
    if (source) source.push({ neighborId: edge.targetNodeId, edge });
    else adjacency.set(edge.sourceNodeId, [{ neighborId: edge.targetNodeId, edge }]);
    const target = adjacency.get(edge.targetNodeId);
    if (target) target.push({ neighborId: edge.sourceNodeId, edge });
    else adjacency.set(edge.targetNodeId, [{ neighborId: edge.sourceNodeId, edge }]);
  }
  return { nodeById, adjacency };
}

function buildGraphContext(path: Path): string {
  const parts: string[] = [];
  for (const step of path) {
    const props = Object.entries(step.node.properties)
      .slice(0, 3)
      .map(([k, v]) => `${k}=${v}`)
      .join(", ");
    let line = `[${step.node.type}] ${step.node.name}`;
    if (props) line += ` (${props})`;
    if (step.edge) {
      line += ` --${step.edge.type}-->`;
      if (step.edge.context?.reasoning) {
        line += ` [${step.edge.context.reasoning}]`;
      }
      if (step.edge.tvalid) {
        line += ` @${step.edge.tvalid}`;
      }
    }
    parts.push(line);
  }
  return parts.join(" ");
}

function averageWeight(path: Path): number {
  const weights = path.filter((s) => s.edge).map((s) => s.edge!.weight);
  if (weights.length === 0) return 0.5;
  return weights.reduce((a, b) => a + b, 0) / weights.length;
}

export class GraphRetrieval {
  constructor(private kv: StateKV) {}

  // 读全量节点与边。
  //
  // 这在修复前是致命操作：18.6K 节点 / 33.2K 边的热行合计 362.8MB JSON，
  // 同步解析把 iii 心跳（约 0.35s）饿死，worker 被判死亡，调用方收到
  // "Invocation stopped"。来源数组外置之后（见 state/graph-provenance.ts）
  // 同一对 scope 只剩 11.7MB，全量枚举重新变成一个便宜操作。
  //
  // 之所以回到「全量枚举 + 内存过滤」而不是给每个查询形状配一套索引：读取方
  // 有一处索引需要，就有十处不需要，而把体积从源头降下来之后，所有这些读取方
  // 一行都不用改。索引只解决已知的查询形状，减小体积解决全部。
  private async loadGraph(): Promise<{ nodes: GraphNode[]; edges: GraphEdge[] }> {
    const [rawNodes, rawEdges] = await Promise.all([
      this.kv.list<GraphNode>(KV.graphNodes),
      this.kv.list<GraphEdge>(KV.graphEdges),
    ]);
    return {
      nodes: rawNodes.filter((n) => !n.stale),
      edges: rawEdges.filter((e) => !e.stale),
    };
  }

  async searchByEntities(
    entityNames: string[],
    maxDepth = 2,
    maxResults = 20,
  ): Promise<GraphRetrievalResult[]> {
    if (entityNames.length === 0) return [];
    const { nodes, edges } = await this.loadGraph();
    if (nodes.length === 0) return [];

    const lowered = entityNames.map((e) => e.toLowerCase());
    const matchingNodes = nodes.filter((n) => {
      const nameLower = n.name.toLowerCase();
      return lowered.some(
        (e) => nameLower.includes(e) || e.includes(nameLower),
      );
    });
    if (matchingNodes.length === 0) return [];

    const index = buildGraphIndex(nodes, edges);
    return this.collectResults({
      index,
      startNodes: matchingNodes,
      maxDepth,
      maxResults,
      seedObs: new Set<string>(),
      scoreOf: (path) => averageWeight(path) * (1 / path.length),
      emitStartNodeObs: true,
    });
  }

  async expandFromChunks(
    obsIds: string[],
    maxDepth = 1,
    maxResults = 10,
  ): Promise<GraphRetrievalResult[]> {
    if (obsIds.length === 0) return [];

    // 种子节点改从反向索引取，不再全量扫节点。
    //
    // 原实现在 18.6K 个节点里逐个检查 sourceObservationIds 是否命中种子 ——
    // 482 万个 obsId 引用，单次就是上百毫秒的纯 CPU，还要先把 132.8MB 读进
    // 内存。索引是每个 obsId 有界的（OBS_INDEX_CAP 个节点），一次 getMany
    // 就够，且精度更高：索引里留下的是抽取出该观测时真正命名的实体，而不是
    // 后续所有共现过的节点。
    const seedNodeIds = await findNodeIdsByObsIds(this.kv, obsIds);
    if (seedNodeIds.length === 0) return [];

    const [seedRows, allNodes, allEdges] = await Promise.all([
      this.kv.getMany<GraphNode>(KV.graphNodes, seedNodeIds),
      this.kv.list<GraphNode>(KV.graphNodes),
      this.kv.list<GraphEdge>(KV.graphEdges),
    ]);
    const nodes = allNodes.filter((n) => !n.stale);
    const edges = allEdges.filter((e) => !e.stale);

    // 种子节点必须仍然存在且未失效 —— 索引是单调追加的，节点可能已经被
    // 级联标记为 stale。
    const seeds: GraphNode[] = [];
    for (const id of seedNodeIds) {
      const row = seedRows.get(id);
      if (row && !row.stale) seeds.push(row);
    }
    if (seeds.length === 0) return [];

    const index = buildGraphIndex(nodes, edges);
    return this.collectResults({
      index,
      startNodes: seeds,
      maxDepth,
      maxResults,
      // 种子观测本身已经是被检索命中的结果，不再作为扩展结果回吐。
      seedObs: new Set(obsIds),
      scoreOf: (path) => 0.5 * (1 / (path.length + 1)),
      emitStartNodeObs: false,
    });
  }

  async temporalQuery(
    entityName: string,
    asOf?: string,
  ): Promise<{
    entity: GraphNode | null;
    currentState: GraphEdge[];
    history: GraphEdge[];
  }> {
    const { nodes, edges } = await this.loadGraph();

    const entity = nodes.find(
      (n) => n.name.toLowerCase() === entityName.toLowerCase(),
    );
    if (!entity) return { entity: null, currentState: [], history: [] };

    const relatedEdges = edges.filter(
      (e) => e.sourceNodeId === entity.id || e.targetNodeId === entity.id,
    );

    // 返回值里带实体本身与相关边，两侧都要把来源数组补回去 —— 单个实体
    // 的关联边数量受度数约束（实测全图最大度 333），不是全量规模。
    const [hydratedEntity] = await hydrateNodesObs(this.kv, [entity]);
    const hydratedEdges = await hydrateEdgesObs(this.kv, relatedEdges);

    if (!asOf) {
      const latestEdges = this.getLatestEdges(hydratedEdges);
      const historicalEdges = hydratedEdges.filter(
        (e) => !latestEdges.some((le) => le.id === e.id),
      );
      return {
        entity: hydratedEntity ?? entity,
        currentState: latestEdges,
        history: historicalEdges,
      };
    }

    const asOfDate = new Date(asOf).getTime();
    const validEdges = hydratedEdges.filter((e) => {
      const commitDate = new Date(e.tcommit || e.createdAt).getTime();
      if (commitDate > asOfDate) return false;
      if (e.tvalid) {
        const validDate = new Date(e.tvalid).getTime();
        if (validDate > asOfDate) return false;
      }
      if (e.tvalidEnd) {
        const endDate = new Date(e.tvalidEnd).getTime();
        if (endDate < asOfDate) return false;
      }
      return true;
    });

    return {
      entity: hydratedEntity ?? entity,
      currentState: this.getLatestEdges(validEdges),
      history: validEdges,
    };
  }

  // 遍历 + 发分的公共部分。
  //
  // 与旧实现的差别只有一处：来源数组不再随行返回，需要在发射前补回来。但补全
  // 一旦变成「按需」的，就带来一个顺序问题 —— 反过来说，也正是这次重写提供了
  // 一个机会：把发射顺序从「遍历顺序」改成「得分顺序」，从而让 obsId 的归属
  // 只由得分决定。
  //
  // 旧实现在 discovery 顺序下发射，而 obsId 的归属权先到先得：两个节点共享一个
  // obsId 时，分数归给先被遍历到的那个，与它是不是更强的那条路径无关。那不是
  // 设计，是历史。现在改为按得分从高到低发射，一个观测归给支撑它的最高分路径。
  private async collectResults(options: {
    index: GraphIndex;
    startNodes: readonly GraphNode[];
    maxDepth: number;
    maxResults: number;
    seedObs: Set<string>;
    scoreOf: (path: Path) => number;
    emitStartNodeObs: boolean;
  }): Promise<GraphRetrievalResult[]> {
    const {
      index,
      startNodes,
      maxDepth,
      maxResults,
      seedObs,
      scoreOf,
      emitStartNodeObs,
    } = options;
    if (maxResults <= 0 || startNodes.length === 0) return [];

    // 第一步：只算候选与得分，不碰状态。
    //
    // 得分只依赖路径（长度 + 边权），提前算出来的整个候选集是零 I/O 的。把补全
    // 推到第二步之后，才有可能按得分裁剪 —— 这是旧实现做不到的，它必须先把
    // 每个可达节点的来源数组都读出来才知道谁分高。
    //
    // 同一个节点被多条路径到达时只保留最高分的那条：旧实现在 discovery 顺序下
    // 也是「第一条到达的路径胜出」，区别只在于这里用的是得分最优而不是代价最早。
    const bestByNode = new Map<
      string,
      { node: GraphNode; path: Path; score: number }
    >();
    for (const start of startNodes) {
      for (const path of this.dijkstraTraversal(index, start.id, maxDepth)) {
        const node = path[path.length - 1]!.node;
        const score = scoreOf(path);
        const prev = bestByNode.get(node.id);
        if (!prev || score > prev.score) bestByNode.set(node.id, { node, path, score });
      }
    }

    // 起点节点自己的观测不走路径打分：score=1.0、pathLength=0，上下文就是实体
    // 本身（不带属性，因为它不是「一条走到这里的路径」）。1.0 严格高于任何路径
    // 分（深度 1 的路径分上界是 0.5），所以排序后它们自然在最前，会优先拿到
    // obsId 的归属权 —— 这正是想要的。
    const startIds = new Set(startNodes.map((n) => n.id));
    const items: Array<{
      node: GraphNode;
      path: Path;
      score: number;
      isStart: boolean;
    }> = [];
    if (emitStartNodeObs) {
      for (const start of startNodes) {
        items.push({ node: start, path: [{ node: start }], score: 1.0, isStart: true });
      }
    }
    for (const candidate of bestByNode.values()) {
      // 起点节点已经以自己的身份发过了，不要再作为某条路径的终点发一次。
      if (emitStartNodeObs && startIds.has(candidate.node.id)) continue;
      items.push({ ...candidate, isStart: false });
    }
    items.sort((a, b) => b.score - a.score);

    // 第二步：按得分从高到低分批补全，凑够 maxResults 且下一批的最高分已经不
    // 可能超过当前第 maxResults 名时早停。
    //
    // 早停是安全的：剩下那些候选中，分数等于阈值的观测不会改变集合的组成（并列
    // 顺序本身就无意义），分数更低的观测进不了前 maxResults。所以结果与「把候选
    // 全部补全」一致，而实际 I/O 通常远小于旧上限 —— 起点节点自己携带的观测往往
    // 就够填满 maxResults，此时只读起点这一批。
    const results: GraphRetrievalResult[] = [];
    const visitedObs = new Set<string>(seedObs);

    for (let offset = 0; offset < items.length; offset += OBS_HYDRATION_BATCH) {
      if (results.length >= maxResults) {
        const threshold = results[maxResults - 1]!.score;
        if (items[offset]!.score <= threshold) break;
      }
      const batch = items.slice(offset, offset + OBS_HYDRATION_BATCH);
      const obsById = await this.readObsFor(index, batch.map((i) => i.node.id));
      for (const item of batch) {
        const obsIds = obsById.get(item.node.id);
        if (!obsIds) continue;
        const graphContext = item.isStart
          ? `[${item.node.type}] ${item.node.name}`
          : buildGraphContext(item.path);
        for (const obsId of obsIds) {
          if (visitedObs.has(obsId)) continue;
          visitedObs.add(obsId);
          results.push({
            obsId,
            sessionId: "",
            score: item.score,
            graphContext,
            pathLength: item.isStart ? 0 : item.path.length,
          });
        }
      }
    }

    results.sort((a, b) => b.score - a.score);
    return results.slice(0, maxResults);
  }

  private async readObsFor(
    index: GraphIndex,
    nodeIds: readonly string[],
  ): Promise<Map<string, string[]>> {
    if (nodeIds.length === 0) return new Map();
    const rows: GraphNode[] = [];
    for (const id of nodeIds) {
      const node = index.nodeById.get(id);
      if (node) rows.push(node);
    }
    return readNodeObsMap(this.kv, rows);
  }

  private getLatestEdges(edges: GraphEdge[]): GraphEdge[] {
    const byKey = new Map<string, GraphEdge[]>();
    for (const e of edges) {
      const key = `${e.sourceNodeId}|${e.targetNodeId}|${e.type}`;
      if (!byKey.has(key)) byKey.set(key, []);
      byKey.get(key)!.push(e);
    }

    const latest: GraphEdge[] = [];
    for (const group of byKey.values()) {
      if (group.length === 0) continue;
      group.sort(
        (a, b) =>
          new Date(b.tcommit || b.createdAt).getTime() -
          new Date(a.tcommit || a.createdAt).getTime(),
      );
      const newest = group.find((e) => e.isLatest !== false) || group[0];
      latest.push(newest);
    }
    return latest;
  }

  // Weighted shortest-path traversal (#328). Replaces the prior BFS,
  // which fell back to edge-count order and ignored the 0.1-1.0 weight
  // attached to every graph edge. Dijkstra over `cost = 1/weight`
  // (cheaper edges = stronger relationships) returns the
  // highest-weighted path to each reachable node within maxDepth.
  //
  // Index construction was hoisted out of this function (it used to
  // rebuild the full node map and adjacency table on every call, i.e.
  // once per start node). Min-heap dequeue is O(log V) per pop
  // (the pre-#328 queue.shift() was O(n) — the dominant cost on graphs
  // above ~200 nodes).
  private dijkstraTraversal(
    index: GraphIndex,
    startNodeId: string,
    maxDepth: number,
  ): Path[] {
    const dist = new Map<string, number>();
    const pathTo = new Map<string, Path>();
    const startNode = index.nodeById.get(startNodeId);
    if (!startNode) return [];

    dist.set(startNodeId, 0);
    pathTo.set(startNodeId, [{ node: startNode }]);

    const heap = new MinHeap<{ nodeId: string; depth: number; cost: number }>(
      (a, b) => a.cost - b.cost,
    );
    heap.push({ nodeId: startNodeId, depth: 0, cost: 0 });

    while (heap.size() > 0) {
      const { nodeId, depth, cost } = heap.pop()!;
      // Skip stale heap entries (cost beaten by a later push).
      if (cost > (dist.get(nodeId) ?? Infinity)) continue;
      if (depth >= maxDepth) continue;

      const neighbors = index.adjacency.get(nodeId) ?? [];
      for (const { neighborId, edge } of neighbors) {
        const nextNode = index.nodeById.get(neighborId);
        if (!nextNode) continue;
        // Clamp weight to avoid division-by-zero on malformed edges;
        // 0.01 is below the documented 0.1 floor.
        const edgeCost = 1 / Math.max(edge.weight, 0.01);
        const newCost = cost + edgeCost;
        if (newCost < (dist.get(neighborId) ?? Infinity)) {
          dist.set(neighborId, newCost);
          pathTo.set(neighborId, [
            ...pathTo.get(nodeId)!,
            { node: nextNode, edge },
          ]);
          heap.push({ nodeId: neighborId, depth: depth + 1, cost: newCost });
        }
      }
    }

    // Drop the startNode's own entry before returning.
    //
    // A length-1 path with no edges is not a real traversal result: it would score
    // through the generic path formula (empty edge weights → avgWeight 0.5), and
    // expandFromChunks would then emit the seed node's *other* observations as if
    // they had been reached from it. searchByEntities happens to be protected by
    // its emitStartNodeObs branch, but the invariant belongs here, at the source.
    pathTo.delete(startNodeId);
    return Array.from(pathTo.values());
  }
}

// Minimal binary min-heap. Pulled inline so graph-retrieval doesn't
// take a new dependency for the perf-critical inner loop of #328.
// Comparator returns negative when `a` should pop before `b`.
class MinHeap<T> {
  private heap: T[] = [];

  constructor(private compare: (a: T, b: T) => number) {}

  size(): number {
    return this.heap.length;
  }

  push(value: T): void {
    this.heap.push(value);
    this.bubbleUp(this.heap.length - 1);
  }

  pop(): T | undefined {
    if (this.heap.length === 0) return undefined;
    const top = this.heap[0];
    const last = this.heap.pop()!;
    if (this.heap.length > 0) {
      this.heap[0] = last;
      this.sinkDown(0);
    }
    return top;
  }

  private bubbleUp(i: number): void {
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (this.compare(this.heap[i], this.heap[parent]) < 0) {
        [this.heap[i], this.heap[parent]] = [this.heap[parent], this.heap[i]];
        i = parent;
      } else break;
    }
  }

  private sinkDown(i: number): void {
    const n = this.heap.length;
    while (true) {
      const left = 2 * i + 1;
      const right = 2 * i + 2;
      let smallest = i;
      if (left < n && this.compare(this.heap[left], this.heap[smallest]) < 0) {
        smallest = left;
      }
      if (right < n && this.compare(this.heap[right], this.heap[smallest]) < 0) {
        smallest = right;
      }
      if (smallest === i) break;
      [this.heap[i], this.heap[smallest]] = [this.heap[smallest], this.heap[i]];
      i = smallest;
    }
  }
}
