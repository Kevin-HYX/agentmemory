import fs from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';
import { registerWorker } from '/opt/agentmemory/node_modules/iii-sdk/dist/index.mjs';
import { ShardedKV } from '/opt/agentmemory/node_modules/@agentmemory/agentmemory/dist/patch-sharded-kv.mjs';
import { graphRegion, patchSearchBundle } from '/tmp/search-performance-20261011/patch-search.mjs';

const sdk = registerWorker('ws://127.0.0.1:49134', { workerName: 'readonly-search-regression', invocationTimeoutMs: 60000 });
await new Promise(resolve => setTimeout(resolve, 800));
const kv = new ShardedKV(sdk);
const nodes = (await kv.list('mem:graph:nodes')).filter(n => !n.stale);
const edges = (await kv.list('mem:graph:edges')).filter(e => !e.stale);
const source = fs.readFileSync('/opt/agentmemory/node_modules/@agentmemory/agentmemory/dist/index.mjs', 'utf8');
function create(source) {
  let builds = 0;
  const context = { KV: { graphNodes: 'nodes', graphEdges: 'edges' }, __indexBuilds: () => builds++ };
  vm.createContext(context);
  vm.runInContext(graphRegion(source).text.replace('const nodeIndex =', '__indexBuilds(); const nodeIndex ='), context);
  return { retrieval: new context.GraphRetrieval({ list: async scope => scope === 'nodes' ? nodes : edges }), builds: () => builds };
}
const before = create(source), after = create(patchSearchBundle(source));
let started = Date.now();
const a = await before.retrieval.searchByEntities(['AgentMemory'], 2, 3);
const beforeMs = Date.now() - started;
started = Date.now();
const b = await after.retrieval.searchByEntities(['AgentMemory'], 2, 3);
const afterMs = Date.now() - started;
assert.deepEqual(JSON.parse(JSON.stringify(b)), JSON.parse(JSON.stringify(a)));
assert.equal(after.builds(), 1);
assert.ok(afterMs < beforeMs / 3, '真实图谱必须显著消除重复建图耗时');
console.log(JSON.stringify({ nodes: nodes.length, edges: edges.length, beforeMs, afterMs, beforeBuilds: before.builds(), afterBuilds: after.builds(), parity: true, results: b.map(x => x.obsId) }));
process.exit(0);
