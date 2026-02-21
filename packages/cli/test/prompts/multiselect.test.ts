import { describe, expect, test } from "bun:test";
import {
  createMultiSelectState,
  hasAnySelected,
  reduceMultiSelectState,
} from "../../src/prompts/multiselect";

describe("multiselect", () => {
  test("multi-select reducer supports cursor move and toggles", () => {
    let state = createMultiSelectState(3, true);

    state = reduceMultiSelectState(state, "down").state;
    expect(state.cursor).toBe(1);

    state = reduceMultiSelectState(state, "space").state;
    expect(state.selected).toEqual([true, false, true]);

    state = reduceMultiSelectState(state, "toggle_all").state;
    expect(state.selected).toEqual([true, true, true]);

    state = reduceMultiSelectState(state, "toggle_all").state;
    expect(hasAnySelected(state)).toBe(false);

    const submit = reduceMultiSelectState(state, "enter");
    expect(submit.action).toBe("submit");
  });
});
