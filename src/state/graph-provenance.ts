import type { GraphEdge, GraphNode } from "../types.js";
import { KV } from "./schema.js";
import type { StateKV } from "./kv.js";

// ---------------------------------------------------------------------------
// 图谱来源追溯（provenance）的读写
// ---------------------------------------------------------------------------
// 背景见 src/state/schema.ts 里 graphNodeObs / graphEdgeObs 的注释：真实数据
// 上 mem:graph:nodes 的 97%、mem:graph:edges 的 96.6% 体积都是
// sourceObservationIds，把这两份来源数组挪到旁路 scope 之后，热行从 362MB
// 降到 11.7MB，全量枚举不再触发 "Invocation stopped"。
//
// 本模块是这套约定的唯一出入口：
//   - 写：hot row 恒为空数组，真实值写进旁路记录；
//   - 读：优先读旁路记录，缺失时回退到热行内的旧值。
//
// 回退分支是有意保留的兼容路径，不是死代码：从旧版本升级上来的数据在回填
// 完成前，旁路记录还不存在，此时热行里仍带着完整的来源数组。靠这个回退，
// 新代码在回填之前就能正常工作，回填可以在任意时间点执行。

/** 反向索引里每个 obsId 最多保留的节点数，见 schema.ts 的 graphObsIndex。 */
export const OBS_INDEX_CAP = 64;

/** 合并多组 obsId，保序去重。 */
export function unionObs(...lists: ReadonlyArray<readonly string[] | undefined>): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const list of lists) {
    if (!list) continue;
    for (const id of list) {
      if (!id || seen.has(id)) continue;
      seen.add(id);
      out.push(id);
    }
  }
  return out;
}

/**
 * 生成用于持久化的热行：来源数组置空，其余字段原样保留。
 *
 * 热行留空而不是删除字段，是为了让 GraphNode / GraphEdge 的类型契约保持
 * 不变 —— 大量消费方（含外部 API 响应形状、mesh 同步载荷）直接读这个字段。
 */
export function stripNodeObs(node: GraphNode): GraphNode {
  return { ...node, sourceObservationIds: [] };
}

export function stripEdgeObs(edge: GraphEdge): GraphEdge {
  return { ...edge, sourceObservationIds: [] };
}

/** 读单个节点的来源数组，旁路缺失时回退到热行内值。 */
export async function readNodeObs(
  kv: StateKV,
  node: GraphNode,
): Promise<string[]> {
  const side = await kv.get<string[]>(KV.graphNodeObs, node.id);
  if (Array.isArray(side)) return side;
  return node.sourceObservationIds ?? [];
}

/** 读单条边的来源数组，旁路缺失时回退到热行内值。 */
export async function readEdgeObs(
  kv: StateKV,
  edge: GraphEdge,
): Promise<string[]> {
  const side = await kv.get<string[]>(KV.graphEdgeObs, edge.id);
  if (Array.isArray(side)) return side;
  return edge.sourceObservationIds ?? [];
}

/**
 * 批量读来源数组，返回 nodeId -> obsId[]。
 *
 * 未命中旁路记录的节点会回退到热行内的值，所以返回的 Map 一定包含每个输入
 * 节点的条目（除非节点 id 为空）。
 */
export async function readNodeObsMap(
  kv: StateKV,
  nodes: readonly GraphNode[],
): Promise<Map<string, string[]>> {
  const side = await kv.getMany<string[]>(
    KV.graphNodeObs,
    nodes.map((n) => n.id),
  );
  const out = new Map<string, string[]>();
  for (const node of nodes) {
    const obs = side.get(node.id);
    out.set(
      node.id,
      Array.isArray(obs) ? obs : node.sourceObservationIds ?? [],
    );
  }
  return out;
}

/** 批量读边的来源数组，语义同 readNodeObsMap。 */
export async function readEdgeObsMap(
  kv: StateKV,
  edges: readonly GraphEdge[],
): Promise<Map<string, string[]>> {
  const side = await kv.getMany<string[]>(
    KV.graphEdgeObs,
    edges.map((e) => e.id),
  );
  const out = new Map<string, string[]>();
  for (const edge of edges) {
    const obs = side.get(edge.id);
    out.set(
      edge.id,
      Array.isArray(obs) ? obs : edge.sourceObservationIds ?? [],
    );
  }
  return out;
}

/**
 * 批量补回节点的来源数组。
 *
 * 用于「调用方明确需要 obsId」的路径：图谱检索的结果集定义、级联失效判定、
 * 导出/同步/快照的往返保真。只要调用方不读 obsId，就不要调它 —— 这是一次
 * 按行数的状态读取。
 */
export async function hydrateNodesObs<T extends GraphNode>(
  kv: StateKV,
  nodes: readonly T[],
): Promise<T[]> {
  if (nodes.length === 0) return [];
  const obsById = await readNodeObsMap(kv, nodes);
  return nodes.map((node) => ({
    ...node,
    sourceObservationIds: obsById.get(node.id) ?? [],
  }));
}

/** 批量补回边的来源数组，语义同 hydrateNodesObs。 */
export async function hydrateEdgesObs<T extends GraphEdge>(
  kv: StateKV,
  edges: readonly T[],
): Promise<T[]> {
  if (edges.length === 0) return [];
  const obsById = await readEdgeObsMap(kv, edges);
  return edges.map((edge) => ({
    ...edge,
    sourceObservationIds: obsById.get(edge.id) ?? [],
  }));
}

/** 写入节点的来源数组。 */
export async function writeNodeObs(
  kv: StateKV,
  nodeId: string,
  obsIds: readonly string[],
): Promise<void> {
  await kv.set(KV.graphNodeObs, nodeId, [...obsIds]);
}

/** 写入边的来源数组。 */
export async function writeEdgeObs(
  kv: StateKV,
  edgeId: string,
  obsIds: readonly string[],
): Promise<void> {
  await kv.set(KV.graphEdgeObs, edgeId, [...obsIds]);
}

/** 批量写入来源数组，回填与大批量抽取走这条路径。 */
export async function writeNodeObsMany(
  kv: StateKV,
  entries: ReadonlyArray<readonly [string, readonly string[]]>,
): Promise<void> {
  await kv.setMany(
    KV.graphNodeObs,
    entries.map(([id, obs]) => [id, [...obs]] as const),
  );
}

export async function writeEdgeObsMany(
  kv: StateKV,
  entries: ReadonlyArray<readonly [string, readonly string[]]>,
): Promise<void> {
  await kv.setMany(
    KV.graphEdgeObs,
    entries.map(([id, obs]) => [id, [...obs]] as const),
  );
}

/**
 * 维护 obsId -> nodeId[] 反向索引。
 *
 * 每个 obsId 封顶 OBS_INDEX_CAP 条，满了就整条跳过（不驱逐已有序位）。这样
 * 索引一旦成形就稳定不变，同一个 obsId 反复抽取只会得到同一批节点，回填结果
 * 与在线写入结果一致 —— 这对「索引可重建」这件事很重要。
 *
 * 一次抽取只产生一次 getMany + 一次 setMany，不随节点数放大。
 */
export async function indexObsToNodes(
  kv: StateKV,
  nodeIds: readonly string[],
  obsIds: readonly string[],
): Promise<void> {
  if (nodeIds.length === 0 || obsIds.length === 0) return;
  const keys = [...new Set(obsIds)];
  const existing = await kv.getMany<string[]>(KV.graphObsIndex, keys);
  const updates: Array<readonly [string, string[]]> = [];
  for (const obsId of keys) {
    const current = existing.get(obsId) ?? [];
    if (current.length >= OBS_INDEX_CAP) continue;
    const next = [...current];
    for (const nodeId of nodeIds) {
      if (next.length >= OBS_INDEX_CAP) break;
      if (!next.includes(nodeId)) next.push(nodeId);
    }
    if (next.length !== current.length) updates.push([obsId, next]);
  }
  if (updates.length > 0) await kv.setMany(KV.graphObsIndex, updates);
}

/**
 * 反向查询：给定种子 obsId，返回索引里与之关联的节点 id。
 *
 * 返回集合可能同时包含已 stale 或已删除的节点，调用方取节点后自行过滤。
 */
export async function findNodeIdsByObsIds(
  kv: StateKV,
  obsIds: readonly string[],
): Promise<string[]> {
  const unique = [...new Set(obsIds)];
  if (unique.length === 0) return [];
  const found = await kv.getMany<string[]>(KV.graphObsIndex, unique);
  const out = new Set<string>();
  for (const ids of found.values()) {
    for (const id of ids) out.add(id);
  }
  return [...out];
}
