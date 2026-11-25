import type { IIIClient } from 'iii-sdk'
import type { StoragePartitionLayout } from '../types.js'
import { withKeyedLock } from './keyed-mutex.js'
import { decodeCursor, encodeCursor } from './list-query.js'
import { resolveGraphScope, notifyGraphGenerationChange } from './graph-generation.js'
import { ensureGraphConsistency, noteGraphSnapshotWrite } from './graph-consistency.js'
import { withGraphIndexMutation, withGraphSnapshotIndexBarrier, withGraphSnapshotUnknownMutation } from './graph-retrieval-index.js'
import { configuredPartitions, PARTITION_COUNT, PARTITION_LAYOUT_SCOPE, partitionForKey, partitionScope } from './partition-layout.js'

export type StateBackend = 'file' | 'redis'

export interface StateKVOptions {
  backend?: StateBackend
  partitionedScopes?: readonly string[]
}

type UpdateOp = { type: string; path: string; value?: unknown }

type UpdateResult = { old_value: unknown; new_value: unknown; errors: unknown[] }

const LOCAL_UPDATE_OPS = new Set(['set', 'remove', 'merge'])
const UNSAFE_PATHS = new Set(['__proto__', 'constructor', 'prototype'])
const ORDER_FIELDS = ['createdAt', 'timestamp', 'startedAt', 'updatedAt'] as const

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function canApplyLocally(ops: UpdateOp[]): boolean {
  return ops.every(
    (op) =>
      LOCAL_UPDATE_OPS.has(op.type) &&
      typeof (op.path ?? '') === 'string' &&
      !UNSAFE_PATHS.has(op.path ?? '') &&
      (op.type !== 'merge' || isPlainObject(op.value)),
  )
}

export function applyUpdateOps(oldValue: unknown, ops: UpdateOp[]): unknown {
  let current: unknown =
    oldValue === null || oldValue === undefined ? {} : structuredClone(oldValue)
  for (const op of ops) {
    const path = op.path ?? ''
    if (op.type === 'set') {
      if (path === '') current = op.value ?? null
      else if (isPlainObject(current)) current[path] = op.value ?? null
    } else if (op.type === 'remove') {
      if (path === '') current = null
      else if (isPlainObject(current)) delete current[path]
    } else if (op.type === 'merge' && isPlainObject(op.value)) {
      if (path === '') {
        if (isPlainObject(current)) Object.assign(current, op.value)
      } else {
        const root: Record<string, unknown> = isPlainObject(current) ? current : {}
        const existing = root[path]
        const target = isPlainObject(existing) ? existing : {}
        Object.assign(target, op.value)
        root[path] = target
        current = root
      }
    }
  }
  return current
}

const MIN_ID_TIME = Date.UTC(2020, 0, 1)
const MAX_ID_TIME = Date.UTC(2100, 0, 1)

export function generatedIdTime(id: unknown): number | null {
  if (typeof id !== 'string') return null
  const parts = id.split('_')
  if (parts.length < 3) return null
  const segment = parts[parts.length - 2]!
  if (!/^[0-9a-z]{6,10}$/.test(segment)) return null
  const ms = parseInt(segment, 36)
  return ms >= MIN_ID_TIME && ms < MAX_ID_TIME ? ms : null
}

function orderKey(value: unknown): number {
  if (!isPlainObject(value)) return Number.POSITIVE_INFINITY
  const fromId = generatedIdTime(value.id)
  if (fromId !== null) return fromId
  for (const field of ORDER_FIELDS) {
    const raw = value[field]
    if (typeof raw === 'number' && Number.isFinite(raw)) return raw
    if (typeof raw === 'string') {
      const parsed = Date.parse(raw)
      if (!Number.isNaN(parsed)) return parsed
    }
  }
  return Number.POSITIVE_INFINITY
}

function idKey(value: unknown): string {
  if (!isPlainObject(value)) return ''
  const id = value.id ?? value.key
  return typeof id === 'string' ? id : typeof id === 'number' ? String(id) : ''
}

export function insertionOrderKey(value: unknown): { at: number; id: string } {
  return { at: orderKey(value), id: idKey(value) }
}

export function compareInsertionOrder(
  a: ReturnType<typeof insertionOrderKey>,
  b: ReturnType<typeof insertionOrderKey>,
): number {
  if (a.at !== b.at) return a.at < b.at ? -1 : 1
  if (a.id === b.id) return 0
  return a.id < b.id ? -1 : 1
}

export function orderLikeInsertion<T>(values: T[]): T[] {
  return values
    .map((value) => ({ value, ...insertionOrderKey(value) }))
    .sort(compareInsertionOrder)
    .map((entry) => entry.value)
}

export async function collectAll<T = unknown>(kv: StateKV, scope: string): Promise<T[]> {
  if (typeof kv.iterate !== 'function') return kv.list<T>(scope)
  const out: T[] = []
  for await (const page of kv.iterate<T>(scope)) out.push(...page)
  if (kv.backend === 'redis') return orderLikeInsertion(out)
  return out
}

export class StateKV {
  readonly backend: StateBackend
  private readonly partitions: Set<string>
  private readonly validatedLayouts = new Map<string, Promise<void>>()

  constructor(
    private sdk: IIIClient,
    options: StateKVOptions = {},
  ) {
    this.backend = options.backend ?? 'file'
    this.partitions = configuredPartitions(options.partitionedScopes?.join(',') ?? process.env.AGENTMEMORY_PARTITIONED_SCOPES)
  }

  private async isPartitioned(scope: string): Promise<boolean> {
    const base = scope.split(':generation:')[0]!
    if (!this.partitions.has(base)) return false
    let validation = this.validatedLayouts.get(base)
    if (!validation) {
      validation = (async () => {
        const layout = await this.sdk.trigger<unknown, StoragePartitionLayout | null>({
          function_id: 'state::get', payload: { scope: PARTITION_LAYOUT_SCOPE, key: base },
        })
        if (layout?.version !== 2 || layout.partitions !== PARTITION_COUNT) {
          throw new Error(`分区迁移未验证或布局不匹配：${base}`)
        }
      })()
      this.validatedLayouts.set(base, validation)
      validation.catch(() => this.validatedLayouts.delete(base))
    }
    await validation
    return true
  }

  private async recordScope(scope: string, key: string): Promise<string> {
    return await this.isPartitioned(scope) ? partitionForKey(scope, key) : scope
  }

  async get<T = unknown>(scope: string, key: string): Promise<T | null> {
    scope = await resolveGraphScope(this, scope)
    await ensureGraphConsistency(this, scope)
    scope = await this.recordScope(scope, key)
    return this.sdk.trigger<{ scope: string; key: string }, T | null>({
      function_id: 'state::get',
      payload: { scope, key },
    })
  }

  private async mutate<T>(
    scope: string,
    key: string,
    operation: (resolvedScope: string) => Promise<T>,
    knownValue?: { value: unknown },
  ): Promise<T> {
    scope = await resolveGraphScope(this, scope)
    await ensureGraphConsistency(this, scope)
    const write = async () => {
      noteGraphSnapshotWrite(this, scope, key)
      try {
        return await operation(await this.recordScope(scope, key))
      } finally {
        noteGraphSnapshotWrite(this, scope, key)
      }
    }
    const orderedWrite = () => this.backend === 'redis'
      ? withKeyedLock(`state-update:${scope}\u0000${key}`, write)
      : write()
    const result = await withGraphIndexMutation(this, scope, key, () =>
      knownValue
        ? withGraphSnapshotIndexBarrier(this, scope, key, knownValue.value, orderedWrite)
        : withGraphSnapshotUnknownMutation(this, scope, key, orderedWrite),
    )
    await notifyGraphGenerationChange(this, scope, key, await this.isPartitioned(scope))
    return result
  }

  async set<T = unknown>(scope: string, key: string, value: T): Promise<T> {
    return this.mutate(scope, key, (resolvedScope) =>
      this.sdk.trigger<{ scope: string; key: string; value: T }, T>({
        function_id: 'state::set',
        payload: { scope: resolvedScope, key, value },
      }),
      { value },
    )
  }

  async update<T = unknown>(scope: string, key: string, ops: UpdateOp[]): Promise<T> {
    return this.mutate(scope, key, async (resolvedScope) => {
      if (this.backend === 'redis' && canApplyLocally(ops)) {
        const oldValue = await this.sdk.trigger<{ scope: string; key: string }, unknown>({
          function_id: 'state::get', payload: { scope: resolvedScope, key },
        })
        const newValue = applyUpdateOps(oldValue, ops)
        await this.sdk.trigger({ function_id: 'state::set', payload: { scope: resolvedScope, key, value: newValue } })
        const result: UpdateResult = { old_value: oldValue, new_value: newValue, errors: [] }
        return result as T
      }
      return this.sdk.trigger<{ scope: string; key: string; ops: UpdateOp[] }, T>({
        function_id: 'state::update',
        payload: { scope: resolvedScope, key, ops },
      })
    })
  }

  async delete(scope: string, key: string): Promise<void> {
    await this.mutate(scope, key, async (resolvedScope) => {
      await this.sdk.trigger<{ scope: string; key: string }, void>({
        function_id: 'state::delete',
        payload: { scope: resolvedScope, key },
      })
    })
  }

  async list<T = unknown>(scope: string): Promise<T[]> {
    scope = await resolveGraphScope(this, scope)
    await ensureGraphConsistency(this, scope)
    if (!(await this.isPartitioned(scope))) {
      const values = await this.sdk.trigger<{ scope: string }, T[]>({ function_id: 'state::list', payload: { scope } })
      return this.backend === 'redis' && Array.isArray(values) ? orderLikeInsertion(values) : values
    }
    const values: T[] = []
    for (let index = 0; index < PARTITION_COUNT; index++) {
      const shard = await this.sdk.trigger<{ scope: string }, T[]>({
        function_id: 'state::list',
        payload: { scope: partitionScope(scope, index) },
      })
      if (!Array.isArray(shard)) throw new Error(`状态列表返回值无效：${scope}`)
      for (const value of shard) values.push(value)
    }
    return orderLikeInsertion(values)
  }

  async *scan<T = unknown>(scope: string): AsyncGenerator<T> {
    for await (const page of this.iterate<T>(scope)) yield* page
  }

  async listKeys(scope: string): Promise<string[]> {
    scope = await resolveGraphScope(this, scope)
    await ensureGraphConsistency(this, scope)
    return this.rawListKeys(scope)
  }

  async count(scope: string): Promise<number> {
    try {
      return (await this.listKeys(scope)).length
    } catch {
      return (await this.list(scope)).length
    }
  }

  private async rawListKeys(scope: string): Promise<string[]> {
    if (!(await this.isPartitioned(scope))) return this.listPhysicalKeys(scope)
    const keys: string[] = []
    for (let index = 0; index < PARTITION_COUNT; index++) {
      for (const key of await this.listPhysicalKeys(partitionScope(scope, index))) keys.push(key)
    }
    return keys
  }

  private async listPhysicalKeys(scope: string): Promise<string[]> {
    const result = await this.sdk.trigger<{ scope: string }, unknown>({
      function_id: 'state::list_keys',
      payload: { scope },
    })
    if (Array.isArray(result)) return result as string[]
    const keys = (result as { keys?: unknown })?.keys
    return Array.isArray(keys) ? (keys as string[]) : []
  }

  async getMany<T = unknown>(
    scope: string,
    keys: string[],
    opts: { concurrency?: number } = {},
  ): Promise<Array<T | null>> {
    scope = await resolveGraphScope(this, scope)
    await ensureGraphConsistency(this, scope)
    const concurrency = Math.min(64, Math.max(1, Math.floor(opts.concurrency ?? 16)))
    return this.rawGetMany<T>(scope, keys, concurrency, await this.isPartitioned(scope))
  }

  private async rawGetMany<T = unknown>(
    scope: string,
    keys: string[],
    concurrency: number,
    partitioned: boolean,
  ): Promise<Array<T | null>> {
    const out: Array<T | null> = new Array(keys.length).fill(null)
    for (let start = 0; start < keys.length; start += concurrency) {
      const slice = keys.slice(start, start + concurrency)
      const values = await Promise.all(
        slice.map((key) =>
          this.sdk.trigger<{ scope: string; key: string }, T | null>({
            function_id: 'state::get',
            payload: { scope: partitioned ? partitionForKey(scope, key) : scope, key },
          }),
        ),
      )
      for (let i = 0; i < slice.length; i++) out[start + i] = values[i] ?? null
    }
    return out
  }

  async *iterate<T = unknown>(
    scope: string,
    opts: { pageSize?: number } = {},
  ): AsyncGenerator<T[]> {
    const pageSize = Math.min(1000, Math.max(1, Math.floor(opts.pageSize ?? 200)))
    scope = await resolveGraphScope(this, scope)
    await ensureGraphConsistency(this, scope)
    const partitioned = await this.isPartitioned(scope)
    for (let index = 0; index < (partitioned ? PARTITION_COUNT : 1); index++) {
      const physicalScope = partitioned ? partitionScope(scope, index) : scope
      const keys = await this.listPhysicalKeys(physicalScope)
      for (let start = 0; start < keys.length; start += pageSize) {
        const values = await this.rawGetMany<T>(physicalScope, keys.slice(start, start + pageSize), 16, false)
        let page = values.filter((value): value is T => value !== null && value !== undefined)
        if (this.backend === 'redis') page = orderLikeInsertion(page)
        if (page.length) yield page
      }
    }
  }

  async listPage<T = unknown>(
    scope: string,
    opts: { limit: number; cursor?: string; order?: 'desc' | 'asc' },
  ): Promise<{ items: T[]; nextCursor?: string; total: number }> {
    const limit = Math.max(1, Math.floor(opts.limit))
    const direction = opts.order === 'asc' ? 1 : -1
    scope = await resolveGraphScope(this, scope)
    await ensureGraphConsistency(this, scope)
    const keys = await this.rawListKeys(scope)
    const ranked = keys.map((key) => ({ key, at: generatedIdTime(key) }))
    const rank = (entry: { key: string; at: number | null }) =>
      entry.at === null ? Number.NEGATIVE_INFINITY : entry.at
    const compare = (a: { key: string; at: number | null }, b: { key: string; at: number | null }) => {
      if (rank(a) !== rank(b)) return (rank(a) - rank(b)) * direction
      return (a.key < b.key ? -1 : a.key > b.key ? 1 : 0) * direction
    }
    ranked.sort(compare)
    const position = decodeCursor(opts.cursor)
    let start = 0
    if (position?.key) {
      const at = generatedIdTime(position.key)
      start = ranked.findIndex((entry) => compare(entry, { key: position.key!, at }) > 0)
      if (start < 0) start = ranked.length
    }
    const pageKeys = ranked.slice(start, start + limit).map((entry) => entry.key)
    const values = await this.rawGetMany<T>(scope, pageKeys, 16, await this.isPartitioned(scope))
    const items = values.filter((value): value is T => value !== null && value !== undefined)
    const hasMore = start + pageKeys.length < ranked.length
    return {
      items,
      total: ranked.length,
      ...(hasMore && pageKeys.length
        ? { nextCursor: encodeCursor({ key: pageKeys[pageKeys.length - 1]! }) }
        : {}),
    }
  }

  async listGroups(): Promise<string[]> {
    const result = await this.sdk.trigger<Record<string, never>, { groups?: unknown } | null>({
      function_id: 'state::list_groups',
      payload: {},
    })
    const groups = result?.groups
    if (!Array.isArray(groups) || !groups.every((group) => typeof group === 'string')) {
      throw new Error('state::list_groups returned an unexpected shape')
    }
    return groups
  }
}
