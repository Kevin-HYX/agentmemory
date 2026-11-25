import { parentPort, workerData } from "node:worker_threads";
import type { ClusteringLimits, ClusteringReply, ClusteringRequest } from "./reflection-clustering.js";

function buildJaccardClusters(allConcepts: Map<string, Set<string>>, limits: ClusteringLimits): string[][] {
  const conceptList = [...allConcepts.keys()].filter(
    (key) => allConcepts.get(key)!.size >= 2,
  );

  const visited = new Set<string>();
  const clusters: string[][] = [];

  for (const concept of conceptList) {
    if (visited.has(concept) || clusters.length >= limits.maxClusters) break;

    const cluster = [concept];
    visited.add(concept);

    const docsA = allConcepts.get(concept)!;
    for (const other of conceptList) {
      if (cluster.length >= limits.maxConcepts) break;
      if (visited.has(other)) continue;
      const docsB = allConcepts.get(other)!;
      const smaller = docsA.size <= docsB.size ? docsA : docsB;
      const larger = smaller === docsA ? docsB : docsA;
      if (smaller.size / larger.size <= 0.3) continue;
      let intersection = 0;
      for (const id of smaller) {
        if (larger.has(id)) intersection++;
      }
      const union = docsA.size + docsB.size - intersection;
      if (intersection / union > 0.3) {
        cluster.push(other);
        visited.add(other);
      }
    }

    if (cluster.length >= 2) clusters.push(cluster);
  }

  return clusters;
}

const channel = parentPort;
if (!channel) throw new Error("Reflection clustering requires a worker thread");
const limits = workerData as ClusteringLimits;
const concepts = new Map<string, Set<string>>();

function addConcept(term: string, id: string): void {
  const docs = concepts.get(term);
  if (docs) docs.add(id);
  else concepts.set(term, new Set([id]));
}

const reply = (result: ClusteringReply): void => channel.postMessage(result);

channel.on("message", (request: ClusteringRequest) => {
  switch (request.type) {
    case "facts":
      for (const { id, fact } of request.rows) {
        for (const term of fact.toLowerCase().split(/\s+/)) {
          if (term.length > 3) addConcept(term, id);
        }
      }
      break;
    case "lessons":
      for (const { id, tags } of request.rows) {
        for (const tag of tags) addConcept(tag.toLowerCase(), id);
      }
      break;
    case "finish":
      reply({ type: "clusters", clusters: buildJaccardClusters(concepts, limits) });
      channel.close();
      return;
  }

  reply({ type: "ready" });
});

reply({ type: "ready" });
