import { setImmediate } from "node:timers/promises";
import { describe, expect, it } from "vitest";
import { clusterReflection } from "../src/functions/reflection-clustering.js";

const limits = { maxClusters: 10, maxConcepts: 32 };

describe("reflection clustering worker", () => {
  it("keeps shared fact and lesson postings in their original order across input batches", async () => {
    const facts = Array.from({ length: 260 }, (_, index) => ({
      id: "unrelated-" + index,
      fact: "unrelated" + index,
    }));
    facts.unshift({ id: "fact-1", fact: "Docker INGRESS Docker" });
    facts.push({ id: "fact-2", fact: "docker ingress" });
    const lessons = [{ id: "lesson-1", tags: ["Docker", "ingress"] }];

    await expect(clusterReflection(facts, lessons, limits)).resolves.toEqual([["docker", "ingress"]]);
  });

  it("lets the caller advance and queues concurrent clustering jobs", async () => {
    const facts = Array.from({ length: 4_000 }, (_, index) => ({
      id: "fact-" + index,
      fact: "concept" + Math.floor(index / 2),
    }));
    let finished = false;
    const job = clusterReflection(facts, [], limits).finally(() => { finished = true; });
    const next = clusterReflection([{ id: "a", fact: "queued test" }, { id: "b", fact: "queued test" }], [], limits);
    const results = Promise.all([job, next]);
    await setImmediate();
    expect(finished).toBe(false);
    await expect(results).resolves.toEqual([[], [["queued", "test"]]]);
  });

  it("terminates a timed-out job and admits a new one", async () => {
    const facts = [{ id: "a", fact: "docker ingress" }, { id: "b", fact: "docker ingress" }];
    const active = clusterReflection(facts, [], limits);
    await expect(clusterReflection(facts, [], limits, 1)).rejects.toMatchObject({ code: "reflection_worker_timeout" });
    await expect(active).resolves.toEqual([["docker", "ingress"]]);
    await expect(clusterReflection(facts, [], limits, 1)).rejects.toMatchObject({ code: "reflection_worker_timeout" });
    await expect(clusterReflection([], [], limits)).resolves.toEqual([]);
  });
});
