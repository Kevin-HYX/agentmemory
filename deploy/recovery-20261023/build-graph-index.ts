import { createReadStream, createWriteStream, readFileSync, writeFileSync } from 'node:fs';
import { createInterface } from 'node:readline';
import { once } from 'node:events';
import { createHash, randomUUID } from 'node:crypto';
import { projectGraphIndexRecord } from '../../src/state/graph-retrieval-index.js';

const root = process.argv[2]!;
const mode = process.argv[3] ?? 'project';
const hash = (value: string) => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const knownNodes = new Set(['file','function','concept','error','decision','pattern','library','person','project','preference','location','organization','event']);
const knownEdges = new Set(['uses','imports','modifies','causes','fixes','depends_on','related_to','works_at','prefers','blocked_by','caused_by','optimizes_for','rejected','avoids','located_in','succeeded_by']);
const output = createWriteStream(`${root}/graph-${mode}.jsonl`);
const write = async (stream: typeof output, value: unknown) => { if (!stream.write(JSON.stringify(value)+'\n')) await once(stream,'drain'); };
const close = async (stream: typeof output) => { stream.end(); await once(stream,'finish'); };
if (mode === 'project') {
  const control = { schema:2, buildId:randomUUID(), status:'ready', revision:1, nodes:0, edges:0, historyEdges:0, incompleteRecords:0, nodesByType:{} as Record<string,number>, edgesByType:{} as Record<string,number>, otherTypes:0, updatedAt:new Date().toISOString() };
  const postings=createWriteStream(`${root}/postings.unsorted`);
  for await (const line of createInterface({input:createReadStream(`${root}/canonical.jsonl`),crlfDelay:Infinity})) {
    const [scope,id,value]=JSON.parse(line);
    const kind=scope.startsWith('partition:v2:mem:graph:nodes:') ? 'node' : scope.startsWith('partition:v2:mem:graph:edges:') ? 'edge' : scope==='mem:graph:edge-history' ? 'history' : null;
    if (!kind) continue;
    const record=projectGraphIndexRecord(kind,id,value);
    if (!record) throw new Error(`索引投影失败：${id}`);
    await write(output,['mem:graph:retrieval-data',`${control.buildId}:record:${kind}:${hash(id)}`,record]);
    for (const posting of record.postings) {
      const text=`${posting}\t${hash(id)}\t${JSON.stringify(id)}\n`;
      if (!postings.write(text)) await once(postings,'drain');
    }
    if(record.incomplete) control.incompleteRecords++;
    const row=record.row;
    if(row && (kind!=='edge'||row.isLatest!==false)) {
      control[kind==='node'?'nodes':kind==='edge'?'edges':'historyEdges']++;
      if(kind!=='history') {
        const known=kind==='node'?knownNodes:knownEdges;
        const counts=kind==='node'?control.nodesByType:control.edgesByType;
        const type=known.has(row.type)?row.type:'other'; counts[type]=(counts[type]??0)+1;
        if(type==='other') control.otherTypes++;
      }
    }
  }
  await close(postings);
  writeFileSync(`${root}/graph-control.json`,JSON.stringify(control,null,2));
  console.log(JSON.stringify(control));
} else {
  const control=JSON.parse(readFileSync(`${root}/graph-control.json`,'utf8'));
  let group:string|null=null, head:string|null=null, previous:{hash:string,id:string,previous:string|null}|null=null,count=0,entries=0,roots=0;
  const flush = async () => {
    if(!previous||!group)return;
    await write(output,['mem:graph:retrieval-data',`${control.buildId}:entry:${group}:${previous.hash}`,{id:previous.id,previous:previous.previous,next:null}]);
    await write(output,['mem:graph:retrieval-data',`${control.buildId}:root:${group}`,{head,count}]);
    roots++;
  };
  for await(const line of createInterface({input:createReadStream(`${root}/postings.sorted`),crlfDelay:Infinity})) {
    const [posting,member,encoded]=line.split('\t');
    if(posting!==group) { await flush(); group=posting!;head=member!;previous=null;count=0; }
    if(previous?.hash===member)throw new Error('倒排成员重复');
    if(previous) await write(output,['mem:graph:retrieval-data',`${control.buildId}:entry:${group}:${previous.hash}`,{id:previous.id,previous:previous.previous,next:member}]);
    previous={hash:member!,id:JSON.parse(encoded!),previous:previous?.hash??null};count++;entries++;
  }
  await flush();
  await write(output,['mem:graph:retrieval-control','current',control]);
  console.log(JSON.stringify({entries,roots,nodes:control.nodes,edges:control.edges,incompleteRecords:control.incompleteRecords}));
}
await close(output);
