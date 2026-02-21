import { colorEnabled, style } from "../terminal";
import { runPrompt } from "./runner";

export interface ConfirmPromptOptions {
  title: string;
  message: string;
  defaultValue?: boolean | undefined;
  yesLabel?: string | undefined;
  noLabel?: string | undefined;
  yesHint?: string | undefined;
  noHint?: string | undefined;
  cancelMessage?: string | undefined;
}

export type ConfirmKey = "up" | "down" | "left" | "right" | "toggle" | "enter" | "cancel" | "yes" | "no";
export type ConfirmAction = "none" | "submit" | "cancel";

export interface ConfirmState {
  cursor: 0 | 1;
}

interface ConfirmOption {
  value: boolean;
  label: string;
  hint?: string | undefined;
}

function radioMark(selected: boolean, withColor: boolean): string {
  return selected ? style("(*)", "32;1", withColor) : style("( )", "37;2", withColor);
}

function renderConfirmPrompt(
  state: ConfirmState,
  options: readonly [ConfirmOption, ConfirmOption],
  withColor: boolean,
  title: string,
  message: string,
): string {
  const lines = [
    style(title, "1;36", withColor),
    message,
    "Keys: Up/Down move | Left/Right toggle | y/n quick-select | Enter confirm | Ctrl+C cancel",
    "",
  ];

  for (let index = 0; index < options.length; index += 1) {
    const option = options[index];
    if (!option) {
      continue;
    }
    const selected = index === state.cursor;
    const pointer = selected ? style(">>", "1;36", withColor) : "  ";
    const label = selected ? style(option.label, "1", withColor) : option.label;
    lines.push(`${pointer} ${radioMark(selected, withColor)} ${label}`);
    if (option.hint?.trim()) {
      lines.push(`   ${style(option.hint, "2", withColor)}`);
    }
  }

  return lines.join("\n");
}

function normalizeCursor(cursor: number): ConfirmState["cursor"] {
  return cursor <= 0 ? 0 : 1;
}

export function createConfirmState(defaultValue = true): ConfirmState {
  return { cursor: defaultValue ? 0 : 1 };
}

export function reduceConfirmState(
  state: ConfirmState,
  key: ConfirmKey,
): { state: ConfirmState; action: ConfirmAction } {
  switch (key) {
    case "up":
    case "down":
    case "left":
    case "right":
    case "toggle": {
      return { state: { cursor: normalizeCursor(1 - state.cursor) }, action: "none" };
    }
    case "yes":
      return { state: { cursor: 0 }, action: "submit" };
    case "no":
      return { state: { cursor: 1 }, action: "submit" };
    case "enter":
      return { state, action: "submit" };
    case "cancel":
      return { state, action: "cancel" };
  }
}

function mapKey(key: { name?: string; ctrl?: boolean }): ConfirmKey | null {
  if (key.ctrl && key.name === "c") return "cancel";
  switch (key.name) {
    case "up": return "up";
    case "down": return "down";
    case "left": return "left";
    case "right": return "right";
    case "space": return "toggle";
    case "return": return "enter";
    case "y": return "yes";
    case "n": return "no";
    default: return null;
  }
}

export function promptConfirm(options: ConfirmPromptOptions): Promise<boolean> {
  const confirmOptions: readonly [ConfirmOption, ConfirmOption] = [
    { value: true, label: options.yesLabel ?? "Yes", hint: options.yesHint },
    { value: false, label: options.noLabel ?? "No", hint: options.noHint },
  ];

  let state = createConfirmState(options.defaultValue ?? true);
  const withColor = colorEnabled();

  return runPrompt({
    render: () => renderConfirmPrompt(state, confirmOptions, withColor, options.title, options.message),
    onKey(key) {
      const mapped = mapKey(key);
      if (!mapped) return null;
      const reduced = reduceConfirmState(state, mapped);
      state = reduced.state;
      if (reduced.action === "cancel") return { action: "cancel" };
      if (reduced.action === "submit") return { action: "submit", value: confirmOptions[state.cursor].value };
      return { action: "none" };
    },
    cancelMessage: options.cancelMessage ?? "prompt cancelled",
  });
}
