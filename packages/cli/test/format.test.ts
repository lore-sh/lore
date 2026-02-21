import { describe, expect, test } from "bun:test";
import { formatTimestamp, summarizeCommit } from "../src/format";

describe("format", () => {
  test("formatTimestamp returns ISO string for valid timestamps", () => {
    const ts = new Date("2025-01-15T10:30:00Z").getTime();
    expect(formatTimestamp(ts)).toBe("2025-01-15T10:30:00.000Z");
  });

  test("formatTimestamp returns string for NaN", () => {
    expect(formatTimestamp(NaN)).toBe("NaN");
  });

  test("summarizeCommit maps all fields", () => {
    const entry = {
      commitId: "abc123",
      seq: 1,
      createdAt: 1700000000000,
      kind: "apply" as const,
      message: "test commit",
      parentIds: ["parent1"],
      parentCount: 1,
      stateHashAfter: "hash1",
      schemaHashBefore: "hash0",
      schemaHashAfter: "hash2",
      planHash: "plan0",
      revertible: true,
      revertTargetId: null,
      operations: [],
    };
    const result = summarizeCommit(entry);
    expect(result.commit_id).toBe("abc123");
    expect(result.seq).toBe(1);
    expect(result.kind).toBe("apply");
    expect(result.parent_ids).toEqual(["parent1"]);
    expect(result.revertible).toBe(true);
  });
});
