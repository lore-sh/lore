import { describe, expect, test } from "bun:test";
import { createConfirmState, reduceConfirmState } from "../src/prompt-ui";

describe("prompt-ui", () => {
  test("createConfirmState uses defaultValue", () => {
    expect(createConfirmState(true).cursor).toBe(0);
    expect(createConfirmState(false).cursor).toBe(1);
  });

  test("reduceConfirmState toggles cursor with arrows and space", () => {
    let state = createConfirmState(true);
    state = reduceConfirmState(state, "down").state;
    expect(state.cursor).toBe(1);
    state = reduceConfirmState(state, "right").state;
    expect(state.cursor).toBe(0);
    state = reduceConfirmState(state, "toggle").state;
    expect(state.cursor).toBe(1);
  });

  test("reduceConfirmState supports quick submit keys", () => {
    let state = createConfirmState(false);
    const yes = reduceConfirmState(state, "yes");
    expect(yes.action).toBe("submit");
    expect(yes.state.cursor).toBe(0);

    state = createConfirmState(true);
    const no = reduceConfirmState(state, "no");
    expect(no.action).toBe("submit");
    expect(no.state.cursor).toBe(1);
  });
});
