import { describe, it, expect, vi } from 'vitest';
import type { IIIClient } from 'iii-sdk';
import { StateKV, applyUpdateOps } from '../src/state/kv.js';
import { PARTITION_COUNT, PARTITION_LAYOUT_SCOPE, partitionForKey, partitionScope } from '../src/state/partition-layout.js';
import { pinGraphState, resetGraphGeneration, notifyGraphGenerationChange, setGraphGenerationListener } from '../src/state/graph-generation.js';
import { withKeyedLock } from '../src/state/keyed-mutex.js';

function fixture(scopes = ['mem:insights']) {
  const store = new Map<string, Map<string, unknown>>();
  store.set(PARTITION_LAYOUT_SCOPE, new Map(scopes.map(scope => [scope, { version: 2, partitions: PARTITION_COUNT }])));
  const trigger = vi.fn(async ({ function_id, payload: p }) => {
    let rows = store.get(p.scope);
    if (function_id === 'state::get') return rows?.get(p.key) ?? null;
    if (function_id === 'state::list') return [...(rows?.values() ?? [])];
    if (!rows) store.set(p.scope, rows = new Map());
    if (function_id === 'state::delete') return rows.delete(p.key);
    if (function_id === 'state::update') {
      const old_value = rows.get(p.key);
      const new_value = applyUpdateOps(old_value, p.ops);
      rows.set(p.key, new_value);
      return { old_value, new_value, errors: [] };
    }
    if (function_id === 'state::set') { rows.set(p.key, p.value); return p.value; }
    throw new Error(function_id);
  });
  return { store, trigger, kv: new StateKV({ trigger } as unknown as IIIClient, { partitionedScopes: scopes }) };
}

describe('原生分区状态', () => {
  it('旧图代次的分区通知等待检索锁释放，避免监听器重入死锁', async () => {
    const { kv } = fixture(['mem:graph:nodes']);
    const events: string[] = [];
    setGraphGenerationListener(async () => { events.push('notify'); });
    try {
      await withKeyedLock('graph:retrieval:legacy', async () => {
        await notifyGraphGenerationChange(kv, 'mem:graph:nodes', 'node', true);
        expect(events).toEqual([]);
        events.push('released');
      });
      await new Promise(resolve => setTimeout(resolve, 0));
      expect(events).toEqual(['released', 'notify']);
    } finally { setGraphGenerationListener(null); }
  });

  it('读写、局部更新和删除使用同一条原生记录，不搬运整个分片', async () => {
    const { kv, store, trigger } = fixture();
    await kv.set('mem:insights', '甲', { id: '甲', confidence: 0.5 });
    await kv.update('mem:insights', '甲', [{ type: 'set', path: 'confidence', value: 0.8 }]);
    expect(await kv.get('mem:insights', '甲')).toEqual({ id: '甲', confidence: 0.8 });
    expect(store.get(partitionForKey('mem:insights', '甲'))?.size).toBe(1);
    expect(trigger.mock.calls.filter(([r]) => r.function_id === 'state::list')).toHaveLength(0);
    await kv.delete('mem:insights', '甲');
    expect(await kv.get('mem:insights', '甲')).toBeNull();
  });

  it('分区元数据不存在时拒绝返回空数据，修复元数据后可重试', async () => {
    const { kv, store } = fixture();
    store.delete(PARTITION_LAYOUT_SCOPE);
    await expect(kv.get('mem:insights', 'a')).rejects.toThrow('分区迁移未验证');
    await expect(kv.set('mem:insights', 'a', {})).rejects.toThrow('分区迁移未验证');
    store.set(PARTITION_LAYOUT_SCOPE, new Map([['mem:insights', { version: 2, partitions: PARTITION_COUNT }]]));
    await kv.set('mem:insights', 'a', { id: 'a' });
    expect(await kv.get('mem:insights', 'a')).toEqual({ id: 'a' });
  });

  it('扫描按需读取下一页，提前停止不会预读其余分区', async () => {
    const { kv, store, trigger } = fixture();
    store.set(partitionScope('mem:insights', 0), new Map([['one', { id: 'one' }], ['two', { id: 'two' }]]));
    for await (const row of kv.scan<{ id: string }>('mem:insights')) { expect(row.id).toBe('one'); break; }
    expect(trigger.mock.calls.filter(([r]) => r.function_id === 'state::list')).toHaveLength(1);
  });

  it('扫描无遗漏且不读取废弃的整表 scope', async () => {
    const { kv, trigger } = fixture();
    for (let i = 0; i < 35; i++) await kv.set('mem:insights', `id-${i}`, { id: `id-${i}` });
    const values = await kv.list<{ id: string }>('mem:insights');
    expect(new Set(values.map(row => row.id)).size).toBe(35);
    const scopes = trigger.mock.calls.filter(([r]) => r.function_id === 'state::list').map(([r]) => r.payload.scope);
    expect(scopes).toHaveLength(PARTITION_COUNT);
    expect(scopes).not.toContain('mem:insights');
  });

  it('已固定的图视图在重置后仍读取原来的分区', async () => {
    const { kv, store } = fixture(['mem:graph:nodes']);
    store.set(partitionForKey('mem:graph:nodes', 'node'), new Map([['node', { id: 'node', name: 'old' }]]));
    const pinned = await pinGraphState(kv);
    await resetGraphGeneration(kv, {});
    expect(await kv.get('mem:graph:nodes', 'node')).toBeNull();
    expect(await pinned.get('mem:graph:nodes', 'node')).toEqual({ id: 'node', name: 'old' });
    const rows = [];
    for await (const row of pinned.scan('mem:graph:nodes')) rows.push(row);
    expect(rows).toEqual([{ id: 'node', name: 'old' }]);
  });
});
