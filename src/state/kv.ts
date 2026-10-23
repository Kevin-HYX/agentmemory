import { createHash } from 'node:crypto'
import type { ISdk } from 'iii-sdk'
import { withKeyedLock } from './keyed-mutex.js'
import { logger } from '../logger.js'

// ---------------------------------------------------------------------------
// 大集合分片
// ---------------------------------------------------------------------------
// iii 的 file_based KV 是「一个 scope = 一个 .bin 文件」，所以 get 和 list
// 都必须先把整个文件读进内存再反序列化。图谱节点/边这类十万量级集合实测
// 单文件达到 139MB / 241MB，一次读取就把 iii 的心跳打挂 —— worker 被判
// 死亡，调用方看到的是 "Invocation stopped"。
//
// 这里按 key 的 sha256 前缀把这类 scope 散列到多个子 scope，每个子 scope
// 存一个「行桶」（{ key: value } 映射）。收益有两点：
//   1. 单文件从数百 MB 降到几十 KB，get 只读命中的那一个分片；
//   2. list 分批读取，批与批之间让出事件循环，心跳不再被同步 JSON.parse
//      饿死（这才是 "Invocation stopped" 的直接成因）。
//
// 分片对调用方完全透明：外部依旧按 (scope, key) 读写，不需要知道分片存在。
const DEFAULT_SHARD_BITS = 8
const SHARD_BATCH = 16
const BUCKET_KEY = 'rows'

// 沿用 2026-09-08 那版手工迁移脚本写入的前缀，这样已有的分片数据可以直接
// 被读到，不需要再迁一次。将来若要换成更中性的前缀，需要配套迁移脚本。
const SHARD_PREFIX = 'patch:v1'

// 分片位宽按 scope 的数据体量分配。
//
// 默认 8 位（256 片）对「行内数据不大」的 scope 足够；来源追溯
// （mem:graph:node-obs / edge-obs）单个 key 的 value 是动辄几百个 obsId 的
// 数组，即使总行数只有几万，256 片下每个分片文件仍有 500KB 量级，批量
// hydrate 时会反复解析大文件。这类 scope 提到 12 位（4096 片），把单次
// get 的解析量压到几十 KB。
//
// 注意：位宽必须是 4 的倍数（十六进制字符数 = 位宽 / 4），否则分片名无法
// 对齐。改动已有 scope 的位宽等同于迁移，会让旧分片读不到。
const SHARD_BITS_BY_SCOPE: Readonly<Record<string, number>> = {
  'mem:graph:node-obs': 12,
  'mem:graph:edge-obs': 12,
}

// 需要分片的 scope，键名与 src/state/schema.ts 的 KV 常量保持一致。
const SHARDED_SCOPES: ReadonlySet<string> = new Set([
  'mem:graph:nodes',
  'mem:graph:edges',
  'mem:graph:node-obs',
  'mem:graph:edge-obs',
  'mem:graph:obs-index',
  'mem:insights',
  'mem:semantic',
])

// 批量原语的并发上限。iii 的状态通道是 RPC，单次开销以毫秒计，并发过高
// 会把引擎侧的文件句柄和事件循环压满，取值保留足够裕量。
const BULK_CONCURRENCY = 24

// 未分片 scope 的批量并发上限，刻意比上面低得多。
//
// 未分片 scope 是「一个 scope = 一个文件」，每次 get 都要把整个文件读进来：
// 图谱的名字索引 1.2MB、边键索引 3MB。24 路并发就是 70MB 同时在飞，比单条
// 顺序读更容易把内存顶上去。分片 scope 单条只有几十 KB，用高并发是安全的。
const UNSHARDED_BULK_CONCURRENCY = 4

type RowBucket = Record<string, unknown>

export function isShardedScope(scope: string): boolean {
  return SHARDED_SCOPES.has(scope)
}

export function shardBitsFor(scope: string): number {
  return SHARD_BITS_BY_SCOPE[scope] ?? DEFAULT_SHARD_BITS
}

export function shardCountFor(scope: string): number {
  return 1 << shardBitsFor(scope)
}

export function shardScopeName(scope: string, key: string): string {
  const chars = shardBitsFor(scope) / 4
  const digest = createHash('sha256').update(key).digest('hex').slice(0, chars)
  return `${SHARD_PREFIX}:${scope}:${digest}`
}

export function shardScopeNames(scope: string): string[] {
  const bits = shardBitsFor(scope)
  const chars = bits / 4
  const total = 1 << bits
  const names: string[] = []
  for (let i = 0; i < total; i++) {
    names.push(
      `${SHARD_PREFIX}:${scope}:${i.toString(16).padStart(chars, '0')}`,
    )
  }
  return names
}

// 显式让出一次事件循环。iii 的心跳超时约 0.35s，而一次同步 JSON.parse
// 数百 MB 会直接把它拖死，Promise.race 的超时定时器同样来不及触发。
function yieldToEventLoop(): Promise<void> {
  return new Promise<void>((resolve) => {
    setImmediate(resolve)
  })
}

// ---------------------------------------------------------------------------
// 未迁移数据的兜底读
// ---------------------------------------------------------------------------
// 分片是「需要迁移才能生效」的存储形态：老数据里 mem:graph:nodes 是一个
// 132.8MB 的 mem%3Agraph%3Anodes.bin，分片视角下 256 个子 scope 全是空的。
// 只读分片就会静默返回空 —— 图谱、语义记忆会「凭空消失」，而且没有任何报错，
// 是最难排查的一类故障。
//
// 所以分片读为空时一律回退到单体 scope，并打印一次告警。回退的代价和迁移前
// 完全一样（就是读那个大文件），这意味着还没迁移的语料拿不到本次修复的性能
// 收益 —— 这是事实，告警里会直接说明该做什么。宁可慢且明确，不要快且错。
const legacyWarnedScopes = new Set<string>()

function warnLegacyRead(scope: string, op: string): void {
  if (legacyWarnedScopes.has(scope)) return
  legacyWarnedScopes.add(scope)
  logger.warn('KV shard miss fell back to the unsharded scope', {
    scope,
    op,
    hint:
      `"${scope}" 仍是未分片的单体文件，分片读取为空，已回退到旧路径。` +
      `回退读要整份加载该文件，这正是大语料 OOM 的成因，因此本次修复的性能` +
      `收益只有在完成分片迁移之后才能拿到。迁移是一次性的离线作业（见本仓库` +
      `部署脚本中的 shard 迁移步骤），迁移完成后重启即可消除本告警。`,
  })
}

// 有限并发地跑一批异步任务。保持结果顺序与输入一致。
async function mapLimit<A, B>(
  items: readonly A[],
  limit: number,
  fn: (item: A, index: number) => Promise<B>,
): Promise<B[]> {
  const results = new Array<B>(items.length)
  let cursor = 0
  const workers = new Array(Math.min(limit, items.length))
    .fill(null)
    .map(async () => {
      for (;;) {
        const index = cursor++
        if (index >= items.length) return
        results[index] = await fn(items[index]!, index)
      }
    })
  await Promise.all(workers)
  return results
}

export class StateKV {
  constructor(private sdk: ISdk) {}

  private rawGet<T>(scope: string, key: string): Promise<T | null> {
    return this.sdk.trigger<{ scope: string; key: string }, T | null>({
      function_id: 'state::get',
      payload: { scope, key },
    })
  }

  private rawSet<T>(scope: string, key: string, value: T): Promise<T> {
    return this.sdk.trigger<{ scope: string; key: string; value: T }, T>({
      function_id: 'state::set',
      payload: { scope, key, value },
    })
  }

  private async readBucket(scope: string): Promise<RowBucket> {
    const bucket = await this.rawGet<RowBucket>(scope, BUCKET_KEY)
    return bucket ?? {}
  }

  async get<T = unknown>(scope: string, key: string): Promise<T | null> {
    if (!isShardedScope(scope)) return this.rawGet<T>(scope, key)
    const bucket = await this.readBucket(shardScopeName(scope, key))
    if (Object.hasOwn(bucket, key)) return bucket[key] as T
    // 分片未命中 → 回退单体 scope（仅未迁移数据会命中这条路径）。
    const legacy = await this.rawGet<T>(scope, key)
    if (legacy !== null && legacy !== undefined) {
      warnLegacyRead(scope, 'get')
      return legacy
    }
    return null
  }

  // 批量取回。分片 scope 下按分片分组，同一个分片只读一次；不同分片之间
  // 有限并发。命中不到或值为 null 的 key 不会出现在结果里。
  //
  // 这是 hydrate（按需补回来源数据）的主入口：调用方拿到一堆 key 却只想
  // 读一遍状态，逐条 get 会产生同量级的 RPC 往返。
  async getMany<T = unknown>(
    scope: string,
    keys: readonly string[],
  ): Promise<Map<string, T>> {
    const out = new Map<string, T>()
    const unique = [...new Set(keys)]
    if (unique.length === 0) return out

    if (!isShardedScope(scope)) {
      await mapLimit(unique, UNSHARDED_BULK_CONCURRENCY, async (key) => {
        const value = await this.rawGet<T>(scope, key)
        if (value !== null && value !== undefined) out.set(key, value)
      })
      return out
    }

    const groups = new Map<string, string[]>()
    for (const key of unique) {
      const shard = shardScopeName(scope, key)
      const group = groups.get(shard)
      if (group) group.push(key)
      else groups.set(shard, [key])
    }

    await mapLimit([...groups.entries()], BULK_CONCURRENCY, async ([shard, shardKeys]) => {
      const bucket = await this.readBucket(shard)
      for (const key of shardKeys) {
        if (Object.hasOwn(bucket, key)) out.set(key, bucket[key] as T)
      }
    })

    // 未命中的 key 回退单体 scope（仅未迁移数据会真正读到东西）。
    const missing = unique.filter((key) => !out.has(key))
    if (missing.length > 0) {
      await mapLimit(missing, UNSHARDED_BULK_CONCURRENCY, async (key) => {
        const legacy = await this.rawGet<T>(scope, key)
        if (legacy !== null && legacy !== undefined) {
          out.set(key, legacy)
          warnLegacyRead(scope, 'getMany')
        }
      })
    }
    return out
  }

  async set<T = unknown>(scope: string, key: string, value: T): Promise<T> {
    if (!isShardedScope(scope)) return this.rawSet<T>(scope, key, value)
    const target = shardScopeName(scope, key)
    return withKeyedLock(target, async () => {
      const bucket = await this.readBucket(target)
      bucket[key] = value
      await this.rawSet(target, BUCKET_KEY, bucket)
      return value
    })
  }

  // 批量写入。分片 scope 下按分片分组，每个分片只做一次「读桶 + 改 + 写回」。
  //
  // 逐条 set 的问题是读改写放大：向同一个分片写 N 行就要把整个分片文件
  // 读解析、写回 N 次。回填场景（几万行、每片几十行）下这个放大是数量级的
  // 差别，所以批量原语是必需的，不是锦上添花。
  async setMany<T = unknown>(
    scope: string,
    entries: ReadonlyArray<readonly [string, T]>,
  ): Promise<void> {
    if (entries.length === 0) return

    if (!isShardedScope(scope)) {
      await mapLimit(entries, UNSHARDED_BULK_CONCURRENCY, async ([key, value]) => {
        await this.rawSet(scope, key, value)
      })
      return
    }

    const groups = new Map<string, Array<readonly [string, T]>>()
    for (const entry of entries) {
      const shard = shardScopeName(scope, entry[0])
      const group = groups.get(shard)
      if (group) group.push(entry)
      else groups.set(shard, [entry])
    }

    await mapLimit([...groups.entries()], BULK_CONCURRENCY, async ([shard, group]) => {
      await withKeyedLock(shard, async () => {
        const bucket = await this.readBucket(shard)
        for (const [key, value] of group) bucket[key] = value
        await this.rawSet(shard, BUCKET_KEY, bucket)
      })
    })
  }

  async update<T = unknown>(
    scope: string,
    key: string,
    ops: Array<{ type: string; path: string; value?: unknown }>,
  ): Promise<T> {
    if (!isShardedScope(scope)) {
      return this.sdk.trigger<
        { scope: string; key: string; ops: Array<{ type: string; path: string; value?: unknown }> },
        T
      >({
        function_id: 'state::update',
        payload: { scope, key, ops },
      })
    }
    const target = shardScopeName(scope, key)
    return withKeyedLock(target, async () => {
      const bucket = await this.readBucket(target)
      // 分片里没有这条记录时，它可能只存在于未迁移的单体 scope 里。
      // update 的语义是「目标必须存在」，所以这里也要回退去找，否则对
      // 老数据的 update 会直接抛 "missing record"。
      let current = bucket[key]
      if (current === undefined) {
        const legacy = await this.rawGet<Record<string, unknown>>(scope, key)
        if (legacy !== null && legacy !== undefined) {
          current = legacy
          warnLegacyRead(scope, 'update')
        }
      }
      if (current === undefined) {
        throw new Error(`state:update on missing record ${scope}/${key}`)
      }
      const next = { ...(current as Record<string, unknown>) }
      for (const op of ops) {
        if (op.type !== 'set') {
          throw new Error(`state:update unsupported operation ${op.type}`)
        }
        next[op.path] = op.value
      }
      // 结果写回分片。老值留在单体 scope 里不影响读取 —— 分片命中优先，
      // 等于这次写入顺手把这条记录迁移了。
      bucket[key] = next
      await this.rawSet(target, BUCKET_KEY, bucket)
      return next as T
    })
  }

  async delete(scope: string, key: string): Promise<void> {
    if (!isShardedScope(scope)) {
      return this.sdk.trigger<{ scope: string; key: string }, void>({
        function_id: 'state::delete',
        payload: { scope, key },
      })
    }
    const target = shardScopeName(scope, key)
    await withKeyedLock(target, async () => {
      const bucket = await this.readBucket(target)
      delete bucket[key]
      await this.rawSet(target, BUCKET_KEY, bucket)
    })
    // 未迁移数据里同一条记录还留在单体 scope 中，只删分片的话下次读取会
    // 通过回退把它「复活」。这里一并删掉，保证删除的语义在迁移前后一致。
    await this.sdk
      .trigger<{ scope: string; key: string }, void>({
        function_id: 'state::delete',
        payload: { scope, key },
      })
      .catch(() => undefined)
  }

  async list<T = unknown>(scope: string): Promise<T[]> {
    if (!isShardedScope(scope)) {
      return this.sdk.trigger<{ scope: string }, T[]>({
        function_id: 'state::list',
        payload: { scope },
      })
    }
    const out: T[] = []
    const names = shardScopeNames(scope)
    for (let start = 0; start < names.length; start += SHARD_BATCH) {
      const batch = names.slice(start, start + SHARD_BATCH)
      const buckets = await Promise.all(batch.map((name) => this.readBucket(name)))
      for (const bucket of buckets) {
        for (const value of Object.values(bucket)) {
          out.push(value as T)
        }
      }
      await yieldToEventLoop()
    }
    // 分片全空 → 该 scope 可能还是未迁移的单体文件。不回退就会静默返回空，
    // 上层看到的是「图谱/语义记忆整个消失」。
    if (out.length === 0) {
      const legacy = await this.sdk.trigger<{ scope: string }, T[]>({
        function_id: 'state::list',
        payload: { scope },
      })
      if (Array.isArray(legacy) && legacy.length > 0) {
        warnLegacyRead(scope, 'list')
        return legacy
      }
    }
    return out
  }

  // 分片级重写：按分片（而不是按行）遍历，把整桶交给 transform 后一次性
  // 写回。这是维护类作业（回填、迁移、规整）该走的路 —— 逐行 set 会把每个
  // 分片文件读改写 N 次，而这里每个分片只读写一次，且峰值内存只有一个
  // 分片的大小。
  //
  // transform 返回空对象时该分片会被跳过（不产生写回）。允许 transform 内部
  // 写别的 scope，但不要写回正在遍历的 scope 的同一个分片 —— 会自锁。
  //
  // concurrency 默认沿用 list 的分批宽度（16）。transform 内部如果还要发起
  // 批量子写入，把并发压到 2-4，避免「父并发 × 子并发」把引擎侧打满。
  async rewriteScope(
    scope: string,
    transform: (
      rows: Record<string, unknown>,
      shard: string,
    ) => Record<string, unknown> | Promise<Record<string, unknown>>,
    options?: { concurrency?: number },
  ): Promise<{ shards: number; rows: number }> {
    if (!isShardedScope(scope)) {
      throw new Error(
        `state:rewriteScope requires a sharded scope, got "${scope}"`,
      )
    }
    const names = shardScopeNames(scope)
    const width = Math.max(1, options?.concurrency ?? SHARD_BATCH)
    let shards = 0
    let rows = 0
    for (let start = 0; start < names.length; start += width) {
      const batch = names.slice(start, start + width)
      const sizes = await Promise.all(
        batch.map((name) =>
          withKeyedLock(name, async () => {
            const bucket = await this.readBucket(name)
            const size = Object.keys(bucket).length
            if (size === 0) return 0
            const updated = await transform(bucket, name)
            await this.rawSet(name, BUCKET_KEY, updated)
            return Object.keys(updated).length
          }),
        ),
      )
      for (const size of sizes) {
        if (size > 0) {
          shards += 1
          rows += size
        }
      }
      await yieldToEventLoop()
    }
    return { shards, rows }
  }
}
