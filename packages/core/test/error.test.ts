import { describe, expect, test } from "bun:test";
import { CodedError } from "../src";

describe("coded error type guard", () => {
  test("accepts real coded errors and rejects inherited object keys", () => {
    expect(CodedError.is(new CodedError("CONFIG", "config error"))).toBe(true);

    const fake = new Error("fake");
    fake.name = "CodedError";
    Object.defineProperty(fake, "code", {
      value: "toString",
      enumerable: true,
      writable: true,
      configurable: true,
    });

    expect(CodedError.is(fake)).toBe(false);
  });
});
