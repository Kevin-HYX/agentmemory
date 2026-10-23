import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

const START = '//#region src/functions/graph-retrieval.ts';
const END = '//#endregion';
const MARKER = 'function buildSearchGraphIndex(allNodes, allEdges)';

export function graphRegion(source) {
  const start = source.indexOf(START);
  const end = source.indexOf(END, start);
  if (start < 0 || end < 0) throw new Error('缺少预期图谱模块，停止部署');
  return { start, end, text: source.slice(start, end) };
}

export function patchSearchBundle(source) {
  const region = graphRegion(source);
  if (region.text.includes(MARKER)) throw new Error('搜索补丁已经存在，停止重复应用');
  const signature = 'dijkstraTraversal(startNode, allNodes, allEdges, maxDepth) {';
  const method = region.text.indexOf(signature);
  const begin = region.text.indexOf('\n\t\tconst nodeIndex', method);
  const end = region.text.indexOf('\n\t\tconst dist', begin);
  if (method < 0 || begin < 0 || end < 0) throw new Error('遍历函数不符合预期，停止部署');
  const construction = region.text.slice(begin, end);
  let updated = region.text.slice(0, begin) + '\n\t\tconst { nodeIndex, adjacency } = index;' + region.text.slice(end);
  updated = updated.replace(signature, 'dijkstraTraversal(startNode, index, maxDepth) {');
  const replacements = [
    ['for (const startNode of matchingNodes) {', 'const index = buildSearchGraphIndex(allNodes, allEdges);\n\t\tfor (const startNode of matchingNodes) {'],
    ['for (const node of linkedNodes) {', 'const index = buildSearchGraphIndex(allNodes, allEdges);\n\t\tfor (const node of linkedNodes) {'],
    ['this.dijkstraTraversal(startNode, allNodes, allEdges, maxDepth)', 'this.dijkstraTraversal(startNode, index, maxDepth)'],
    ['this.dijkstraTraversal(node, allNodes, allEdges, maxDepth)', 'this.dijkstraTraversal(node, index, maxDepth)'],
  ];
  for (const [before, after] of replacements) {
    if (updated.split(before).length !== 2) throw new Error('搜索函数锚点不唯一，停止部署');
    updated = updated.replace(before, after);
  }
  const classMarker = 'var GraphRetrieval = class {';
  if (updated.split(classMarker).length !== 2) throw new Error('图谱类锚点不唯一，停止部署');
  updated = updated.replace(classMarker, `${MARKER} {${construction}\n\treturn { nodeIndex, adjacency };\n}\n${classMarker}`);
  return source.slice(0, region.start) + updated + source.slice(region.end);
}

export function patchDirectory(root) {
  const files = fs.readdirSync(root).filter(name => name.endsWith('.mjs'));
  const pending = files.map(name => ({ name, source: fs.readFileSync(path.join(root, name), 'utf8') }))
    .filter(file => file.source.includes(START));
  if (!pending.some(file => file.name === 'index.mjs')) throw new Error('缺少生产入口，停止部署');
  const changes = pending.map(file => ({ ...file, updated: patchSearchBundle(file.source) }));
  const sha = value => crypto.createHash('sha256').update(value).digest('hex');
  for (const change of changes) fs.writeFileSync(path.join(root, change.name), change.updated);
  return changes.map(change => ({ file: change.name, before: sha(change.source), after: sha(change.updated) }));
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  console.log(JSON.stringify({ patch: 'search-performance-20261011', bundles: patchDirectory(process.argv[2] || '/opt/agentmemory/node_modules/@agentmemory/agentmemory/dist') }));
}
