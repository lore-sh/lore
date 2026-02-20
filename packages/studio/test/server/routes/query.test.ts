import { describe, expect, test } from "bun:test";
import { parsePositiveInt, singleValue } from "../../../src/server/routes/query";

describe("query helpers", () => {
  test("parsePositiveInt accepts positive digit strings", () => {
    expect(parsePositiveInt("1")).toBe(1);
    expect(parsePositiveInt("00080")).toBe(80);
  });

  test("parsePositiveInt rejects non-strict positive integers", () => {
    expect(parsePositiveInt(undefined)).toBeUndefined();
    expect(parsePositiveInt("")).toBeUndefined();
    expect(parsePositiveInt("0")).toBeUndefined();
    expect(parsePositiveInt("-1")).toBeUndefined();
    expect(parsePositiveInt("1.5")).toBeUndefined();
    expect(parsePositiveInt("10abc")).toBeUndefined();
    expect(parsePositiveInt(" 10")).toBeUndefined();
    expect(parsePositiveInt("10 ")).toBeUndefined();
  });

  test("singleValue returns only first string value", () => {
    expect(singleValue("x")).toBe("x");
    expect(singleValue(["x", "y"])).toBe("x");
    expect(singleValue([])).toBeUndefined();
    expect(singleValue(undefined)).toBeUndefined();
  });
});
