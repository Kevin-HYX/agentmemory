import { createHash } from "node:crypto";
import { KV } from "./schema.js";

export const PARTITION_COUNT = 1024;
export const PARTITION_LAYOUT_SCOPE = KV.storageLayouts;
export const PARTITIONABLE_SCOPES = new Set(["mem:graph:nodes", "mem:graph:edges", "mem:insights", "mem:semantic"]);

export function partitionScope(scope: string, index: number): string {
  return `partition:v2:${scope}:${index.toString(16).padStart(3, "0")}`;
}

export function partitionForKey(scope: string, key: string): string {
  const index = createHash("sha256").update(key).digest().readUInt16BE(0) % PARTITION_COUNT;
  return partitionScope(scope, index);
}

export function configuredPartitions(raw: string | undefined): Set<string> {
  const scopes = new Set((raw ?? "").split(",").map(value => value.trim()).filter(Boolean));
  for (const scope of scopes) {
    if (!PARTITIONABLE_SCOPES.has(scope)) throw new Error(`不支持的分区范围：${scope}`);
  }
  return scopes;
}
