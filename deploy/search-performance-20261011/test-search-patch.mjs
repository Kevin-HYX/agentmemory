import fs from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';
import { graphRegion, patchSearchBundle } from './patch-search.mjs';

const original = fs.readFileSync(process.argv[2], 'utf8');
const expectSingle = process.argv.includes('--expect-single');
function makeRetrieval(source, nodes, edges) {
  let builds = 0;
  const context = { KV: { graphNodes: 'nodes', graphEdges: 'edges' }, __indexBuilds: () => builds++ };
  vm.createContext(context);
  vm.runInContext(graphRegion(source).text.replace('const nodeIndex =', '__indexBuilds(); const nodeIndex ='), context);
  const kv = { list: async scope => scope === 'nodes' ? nodes : edges };
  return { retrieval: new context.GraphRetrieval(kv), builds: () => builds };
}

const nodes = Array.from({ length: 600 }, (_, i) => ({
  id: 'n' + i, name: i < 80 ? 'AgentMemory ' + i : 'entity ' + i, type: 'concept',
  properties: {}, sourceObservationIds: ['obs' + i], stale: false,
}));
const edges = Array.from({ length: 1200 }, (_, i) => ({
  id: 'e' + i, sourceNodeId: 'n' + (i % nodes.length), targetNodeId: 'n' + ((i + 1 + Math.floor(i / nodes.length)) % nodes.length),
  type: 'related', weight: 0.5 + (i % 5) / 10, sourceObservationIds: [], stale: false,
}));
if (expectSingle) {
  const probe = makeRetrieval(original, nodes, edges);
  await probe.retrieval.searchByEntities(['AgentMemory'], 2, 3);
  assert.equal(probe.builds(), 1, '一次查询必须只构建一次整图索引');
  console.log('PASS 单次整图构建');
} else {
  const patched = patchSearchBundle(original);
  for (const [method, args] of [
    ['searchByEntities', [['AgentMemory'], 2, 3]],
    ['searchByEntities', [['entity 59'], 2, 20]],
    ['searchByEntities', [['不存在'], 2, 3]],
    ['expandFromChunks', [['obs0', 'obs10', 'obs79'], 1, 5]],
    ['expandFromChunks', [['不存在'], 1, 5]],
    ['temporalQuery', ['AgentMemory 0']],
  ]) {
    const before = makeRetrieval(original, nodes, edges), after = makeRetrieval(patched, nodes, edges);
    const a = await before.retrieval[method](...args), b = await after.retrieval[method](...args);
    assert.deepEqual(JSON.parse(JSON.stringify(b)), JSON.parse(JSON.stringify(a)), '补丁必须保留检索结果、得分和顺序');
    assert.ok(after.builds() <= 1, '一次查询不能重复构建整图索引');
    console.log(JSON.stringify({ method, beforeBuilds: before.builds(), afterBuilds: after.builds(), parity: true }));
  }
  assert.throws(() => patchSearchBundle(patched), /已经存在/);
  assert.throws(() => patchSearchBundle(original.replace('for (const node of linkedNodes) {', 'for (const node of incompatibleNodes) {')), /锚点/);
  console.log('PASS 结果一致性、重复构建和补丁边界检查');
}
