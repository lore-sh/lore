import { stdout } from "node:process";
import { colorEnabled, style } from "../terminal";
import { runPrompt } from "./runner";

export interface MultiSelectOption<T> {
  id: T;
  label: string;
  hint: string;
}

export type MultiSelectKey = "up" | "down" | "space" | "toggle_all" | "enter" | "cancel";
export type MultiSelectAction = "none" | "submit" | "cancel";

export interface MultiSelectState {
  cursor: number;
  selected: boolean[];
}

function checkbox(selected: boolean, enabled: boolean): string {
  return selected ? style("[✓]", "32;1", enabled) : style("[ ]", "37;2", enabled);
}

function renderMultiSelectPrompt<T>(
  title: string,
  subtitle: string,
  keyHint: string,
  state: MultiSelectState,
  options: ReadonlyArray<MultiSelectOption<T>>,
  withColor: boolean,
): string {
  const selectedCount = state.selected.filter(Boolean).length;
  const lines = [
    style(title, "1;36", withColor),
    subtitle,
    keyHint,
    style(`Selected: ${selectedCount}/${options.length}`, "1;33", withColor),
    "",
  ];

  for (let index = 0; index < options.length; index += 1) {
    const option = options[index];
    if (!option) {
      continue;
    }
    const stateBadge = checkbox(state.selected[index] === true, withColor);
    const pointer = index === state.cursor ? style(">>", "1;36", withColor) : "  ";
    const label = index === state.cursor ? style(option.label, "1", withColor) : option.label;
    const hint = style(option.hint, "2", withColor);
    lines.push(`${pointer} ${stateBadge} ${label}`);
    lines.push(`   ${hint}`);
  }

  return lines.join("\n");
}

export function createMultiSelectState(count: number, selectedByDefault = true): MultiSelectState {
  return { cursor: 0, selected: Array.from({ length: count }, () => selectedByDefault) };
}

export function hasAnySelected(state: MultiSelectState): boolean {
  return state.selected.some(Boolean);
}

export function reduceMultiSelectState(
  state: MultiSelectState,
  key: MultiSelectKey,
): { state: MultiSelectState; action: MultiSelectAction } {
  if (state.selected.length === 0) {
    return { state, action: key === "cancel" ? "cancel" : "none" };
  }
  switch (key) {
    case "up": {
      const cursor = state.cursor === 0 ? state.selected.length - 1 : state.cursor - 1;
      return { state: { ...state, cursor }, action: "none" };
    }
    case "down": {
      const cursor = state.cursor === state.selected.length - 1 ? 0 : state.cursor + 1;
      return { state: { ...state, cursor }, action: "none" };
    }
    case "space": {
      const selected = [...state.selected];
      selected[state.cursor] = !selected[state.cursor];
      return { state: { ...state, selected }, action: "none" };
    }
    case "toggle_all": {
      const shouldSelectAll = state.selected.some((value) => !value);
      const selected = state.selected.map(() => shouldSelectAll);
      return { state: { ...state, selected }, action: "none" };
    }
    case "enter":
      return { state, action: "submit" };
    case "cancel":
      return { state, action: "cancel" };
  }
}

function mapKey(key: { name?: string; ctrl?: boolean }): MultiSelectKey | null {
  if (key.ctrl && key.name === "c") return "cancel";
  switch (key.name) {
    case "up": return "up";
    case "down": return "down";
    case "space": return "space";
    case "return": return "enter";
    case "a": return "toggle_all";
    default: return null;
  }
}

export function promptMultiSelect<T>(options: {
  title: string;
  subtitle: string;
  keyHint: string;
  options: ReadonlyArray<MultiSelectOption<T>>;
  selectedByDefault?: boolean;
  cancelMessage: string;
}): Promise<T[]> {
  let state = createMultiSelectState(options.options.length, options.selectedByDefault ?? true);
  const withColor = colorEnabled();

  return runPrompt({
    render: () =>
      renderMultiSelectPrompt(options.title, options.subtitle, options.keyHint, state, options.options, withColor),
    onKey(key) {
      const mapped = mapKey(key);
      if (!mapped) return null;
      const reduced = reduceMultiSelectState(state, mapped);
      state = reduced.state;
      if (reduced.action === "cancel") return { action: "cancel" };
      if (reduced.action === "submit") {
        if (!hasAnySelected(state)) {
          stdout.write("\x07");
          return null;
        }
        const selected: T[] = [];
        for (let index = 0; index < state.selected.length; index += 1) {
          if (state.selected[index]) {
            const opt = options.options[index];
            if (opt) selected.push(opt.id);
          }
        }
        return { action: "submit", value: selected };
      }
      return { action: "none" };
    },
    cancelMessage: options.cancelMessage,
  });
}
