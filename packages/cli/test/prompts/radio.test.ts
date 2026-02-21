import { describe, expect, test } from "bun:test";
import { createRadioState, reduceRadioState } from "../../src/prompts/radio";

describe("radio", () => {
  test("radio reducer wraps cursor", () => {
    let state = createRadioState(0, 2);
    state = reduceRadioState(state, "down", 2);
    expect(state.cursor).toBe(1);
    state = reduceRadioState(state, "down", 2);
    expect(state.cursor).toBe(0);
    state = reduceRadioState(state, "up", 2);
    expect(state.cursor).toBe(1);
  });
});
