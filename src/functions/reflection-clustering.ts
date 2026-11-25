import { Worker } from "node:worker_threads";
import { withKeyedLock } from "../state/keyed-mutex.js";
import type { Lesson, SemanticMemory } from "../types.js";

const CLUSTERING_TIMEOUT_MS = 120_000;
const BATCH_SIZE = 256;
const CLUSTERING_LOCK = "reflection:jaccard";

type SemanticFact = Pick<SemanticMemory, "id" | "fact">;
type LessonTags = Pick<Lesson, "id" | "tags">;

export type ClusteringLimits = { maxClusters: number; maxConcepts: number };
export type ClusteringRequest =
  | { type: "facts"; rows: SemanticFact[] }
  | { type: "lessons"; rows: LessonTags[] }
  | { type: "finish" };
export type ClusteringReply =
  | { type: "ready" }
  | { type: "clusters"; clusters: string[][] };

export class ReflectionClusteringError extends Error {
  constructor(
    readonly code: "reflection_worker_timeout" | "reflection_worker_failed",
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
  }
}

function startClusteringWorker(workerData: ClusteringLimits): Worker {
  if (!import.meta.url.endsWith(".ts")) {
    return new Worker(new URL("./functions/reflection-worker.mjs", import.meta.url), { execArgv: [], workerData });
  }

  const loader = JSON.stringify(import.meta.resolve("tsx/esm/api"));
  const entry = JSON.stringify(new URL("./reflection-worker.ts", import.meta.url).href);
  const context = JSON.stringify(import.meta.url);
  const bootstrap = "import { tsImport } from " + loader + "; await tsImport(" + entry + ", " + context + ");";
  return new Worker(new URL("data:text/javascript," + encodeURIComponent(bootstrap)), { execArgv: [], workerData });
}

async function runClusteringWorker(
  facts: readonly SemanticFact[],
  lessons: readonly LessonTags[],
  limits: ClusteringLimits,
  signal: AbortSignal,
): Promise<string[][]> {
  signal.throwIfAborted();
  let thread: Worker | undefined;
  let abort: (() => void) | undefined;
  try {
    const worker = thread = startClusteringWorker(limits);
    return await new Promise<string[][]>((resolve, reject) => {
      let factOffset = 0;
      let lessonOffset = 0;

      worker.on("message", (reply: ClusteringReply) => {
        if (reply.type === "clusters") {
          resolve(reply.clusters);
          return;
        }

        try {
          let request: ClusteringRequest;
          if (factOffset < facts.length) {
            const rows = facts.slice(factOffset, factOffset + BATCH_SIZE).map(({ id, fact }) => ({ id, fact }));
            factOffset += rows.length;
            request = { type: "facts", rows };
          } else if (lessonOffset < lessons.length) {
            const rows = lessons.slice(lessonOffset, lessonOffset + BATCH_SIZE).map(({ id, tags }) => ({ id, tags }));
            lessonOffset += rows.length;
            request = { type: "lessons", rows };
          } else {
            request = { type: "finish" };
          }
          worker.postMessage(request);
        } catch (error) {
          reject(error);
        }
      });
      worker.once("error", reject);
      worker.once("exit", (code) => reject(new Error("Reflection worker exited before returning clusters (code " + code + ")")));
      abort = () => reject(signal.reason);
      signal.addEventListener("abort", abort, { once: true });
    });
  } catch (error) {
    if (error instanceof ReflectionClusteringError) throw error;
    throw new ReflectionClusteringError("reflection_worker_failed", "Reflection clustering worker failed", { cause: error });
  } finally {
    if (abort) signal.removeEventListener("abort", abort);
    await thread?.terminate();
  }
}

export async function clusterReflection(
  facts: readonly SemanticFact[],
  lessons: readonly LessonTags[],
  limits: ClusteringLimits,
  timeoutMs = CLUSTERING_TIMEOUT_MS,
): Promise<string[][]> {
  const controller = new AbortController();
  const { signal } = controller;
  let input: { facts: readonly SemanticFact[]; lessons: readonly LessonTags[] } | undefined = { facts, lessons };
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timedOut = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      const error = new ReflectionClusteringError("reflection_worker_timeout", "Reflection clustering timed out");
      input = undefined;
      controller.abort(error);
      reject(error);
    }, timeoutMs);
  });

  try {
    const queued = withKeyedLock(CLUSTERING_LOCK, async () => {
      const job = input;
      input = undefined;
      if (!job) throw signal.reason;
      return runClusteringWorker(job.facts, job.lessons, limits, signal);
    });
    return await Promise.race([queued, timedOut]);
  } finally {
    clearTimeout(timer);
  }
}
