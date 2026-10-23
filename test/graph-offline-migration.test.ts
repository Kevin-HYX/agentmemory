import { mkdtempSync, writeFileSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { execFileSync } from 'node:child_process';
import { describe, it, expect } from 'vitest';
import { mockKV } from './helpers/mocks.js';
import { createGraphIndexReader, reconcileGraphIndexRecord } from '../src/state/graph-retrieval-index.js';
import type { StateKV } from '../src/state/kv.js';

describe('离线图索引迁移', () => {
  it('生成的链表可被上游读取，并支持之后的改名和删除', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'agentmemory-index-'));
    try {
      const script = resolve('deploy/recovery-20261023/build-graph-index.ts');
      const common = { createdAt: '2026-10-23T00:00:00Z', sourceObservationIds: ['obs'] };
      const alpha = { ...common, id: 'a', type: 'concept', name: 'Alpha', properties: {} };
      const beta = { ...common, id: 'b', type: 'concept', name: 'Beta', properties: {} };
      const edge = { ...common, id: 'e', type: 'related_to', sourceNodeId: 'a', targetNodeId: 'b', weight: 1 };
      const rows = [
        ['partition:v2:mem:graph:nodes:000', 'a', alpha],
        ['partition:v2:mem:graph:nodes:001', 'b', beta],
        ['partition:v2:mem:graph:edges:002', 'e', edge],
      ];
      writeFileSync(join(directory, 'canonical.jsonl'), rows.map(row => JSON.stringify(row)).join('\n') + '\n');
      execFileSync(process.execPath, ['--import', 'tsx', script, directory, 'project']);
      const sorted = readFileSync(join(directory, 'postings.unsorted'), 'utf8').trim().split('\n').sort().join('\n') + '\n';
      writeFileSync(join(directory, 'postings.sorted'), sorted);
      execFileSync(process.execPath, ['--import', 'tsx', script, directory, 'postings']);
      const kv = mockKV() as unknown as StateKV;
      for (const file of ['graph-project.jsonl', 'graph-postings.jsonl']) {
        for (const line of readFileSync(join(directory, file), 'utf8').trim().split('\n')) {
          const [scope, key, value] = JSON.parse(line);
          await kv.set(scope, key, value);
        }
      }
      let reader = (await createGraphIndexReader(kv))!;
      expect(new Set((await reader.posting('nodes', '', { limit: 10 })).ids)).toEqual(new Set(['a', 'b']));
      expect((await reader.posting('adjacency', 'a', { limit: 10 })).ids).toEqual(['e']);
      expect((await reader.getNode('a'))?.name).toBe('Alpha');
      expect(await reader.finish()).toBe(true);
      await reconcileGraphIndexRecord(kv, 'node', 'a', { ...alpha, name: 'Gamma' } as never);
      await reconcileGraphIndexRecord(kv, 'edge', 'e', null);
      reader = (await createGraphIndexReader(kv))!;
      expect((await reader.posting('name', 'Alpha', { limit: 10 })).ids).toEqual([]);
      expect((await reader.posting('name', 'Gamma', { limit: 10 })).ids).toEqual(['a']);
      expect((await reader.posting('adjacency', 'a', { limit: 10 })).ids).toEqual([]);
      expect(await reader.finish()).toBe(true);
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  }, 15000);
});
