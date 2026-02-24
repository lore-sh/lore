import { describe, expect, test } from "bun:test";
import { canonicalJson, sha256Hex } from "../src/hash";

describe("hash", () => {
  test("canonicalJson sorts object keys recursively", () => {
    const a = canonicalJson({ b: 1, a: { d: 4, c: 3 } });
    const b = canonicalJson({ a: { c: 3, d: 4 }, b: 1 });
    expect(a).toBe(b);
  });

  test("sha256Hex is deterministic for equivalent objects", () => {
    const left = sha256Hex({ z: 1, a: 2 });
    const right = sha256Hex({ a: 2, z: 1 });
    expect(left).toBe(right);
  });
});
