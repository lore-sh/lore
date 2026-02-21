import { stdout } from "node:process";
import { colorEnabled, style } from "../terminal";
import { runPrompt } from "./runner";

export interface RadioOption<T extends string> {
  id: T;
  label: string;
  hint: string;
}

export type RadioKey = "up" | "down" | "enter" | "cancel";

export interface RadioState {
  cursor: number;
}

function radioMark(selected: boolean, withColor: boolean): string {
  return selected ? style("(*)", "32;1", withColor) : style("( )", "37;2", withColor);
}

function renderRadioPrompt<T extends string>(
  title: string,
  subtitle: string,
  state: RadioState,
  options: ReadonlyArray<RadioOption<T>>,
  withColor: boolean,
): string {
  const lines = [
    style(title, "1;36", withColor),
    subtitle,
    "Keys: Up/Down move | Enter confirm | Ctrl+C cancel",
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
    const hint = style(option.hint, "2", withColor);
    lines.push(`${pointer} ${radioMark(selected, withColor)} ${label}`);
    lines.push(`   ${hint}`);
  }

  return lines.join("\n");
}

export function createRadioState(cursor = 0, optionCount = 0): RadioState {
  if (optionCount <= 0) {
    return { cursor: 0 };
  }
  if (cursor < 0 || cursor >= optionCount) {
    return { cursor: 0 };
  }
  return { cursor };
}

export function reduceRadioState(state: RadioState, key: RadioKey, optionCount = 0): RadioState {
  if (optionCount <= 0) {
    return state;
  }
  switch (key) {
    case "up":
      return { cursor: state.cursor === 0 ? optionCount - 1 : state.cursor - 1 };
    case "down":
      return { cursor: state.cursor === optionCount - 1 ? 0 : state.cursor + 1 };
    case "enter":
    case "cancel":
      return state;
  }
}

function mapKey(key: { name?: string; ctrl?: boolean }): RadioKey | null {
  if (key.ctrl && key.name === "c") return "cancel";
  switch (key.name) {
    case "up": return "up";
    case "down": return "down";
    case "return": return "enter";
    default: return null;
  }
}

export function promptRadioSelection<T extends string>(options: {
  title: string;
  subtitle: string;
  options: ReadonlyArray<RadioOption<T>>;
  cancelMessage: string;
}): Promise<T> {
  let state = createRadioState(0, options.options.length);
  const withColor = colorEnabled();

  return runPrompt({
    render: () => renderRadioPrompt(options.title, options.subtitle, state, options.options, withColor),
    onKey(key) {
      const mapped = mapKey(key);
      if (!mapped) return null;
      if (mapped === "cancel") return { action: "cancel" };
      if (mapped === "enter") {
        const selected = options.options[state.cursor];
        if (!selected) {
          stdout.write("\x07");
          return null;
        }
        return { action: "submit", value: selected.id };
      }
      state = reduceRadioState(state, mapped, options.options.length);
      return { action: "none" };
    },
    cancelMessage: options.cancelMessage,
  });
}
