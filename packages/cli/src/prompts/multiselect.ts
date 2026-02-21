import { clearLine, clearScreenDown, cursorTo, emitKeypressEvents, moveCursor } from "node:readline";
import { stdin, stdout } from "node:process";
import { colorEnabled, style } from "../terminal";

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
  if (selected) {
    return style("[✓]", "32;1", enabled);
  }
  return style("[ ]", "37;2", enabled);
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

export async function promptMultiSelect<T>(options: {
  title: string;
  subtitle: string;
  keyHint: string;
  options: ReadonlyArray<MultiSelectOption<T>>;
  selectedByDefault?: boolean;
  cancelMessage: string;
}): Promise<T[]> {
  if (!stdin.isTTY || !stdout.isTTY) {
    throw new Error("interactive selection requires a TTY");
  }

  let state = createMultiSelectState(options.options.length, options.selectedByDefault ?? true);
  let renderedLines = 0;
  const withColor = colorEnabled();

  const redraw = (): void => {
    if (renderedLines > 0) {
      if (renderedLines > 1) {
        moveCursor(stdout, 0, -(renderedLines - 1));
      }
      cursorTo(stdout, 0);
    }

    const lines = renderMultiSelectPrompt(
      options.title,
      options.subtitle,
      options.keyHint,
      state,
      options.options,
      withColor,
    ).split("\n");
    for (let index = 0; index < lines.length; index += 1) {
      const line = lines[index] ?? "";
      clearLine(stdout, 0);
      cursorTo(stdout, 0);
      stdout.write(line);
      if (index < lines.length - 1) {
        stdout.write("\n");
      }
    }
    renderedLines = lines.length;
    clearScreenDown(stdout);
  };

  emitKeypressEvents(stdin);
  stdin.setRawMode(true);
  stdin.resume();
  stdout.write("\x1B[?25l");
  stdout.write("\n");
  redraw();

  return await new Promise<T[]>((resolve, reject) => {
    const cleanup = (): void => {
      stdin.off("keypress", onKeypress);
      if (stdin.isRaw) {
        stdin.setRawMode(false);
      }
      stdin.pause();
      stdout.write("\x1B[?25h");
      stdout.write("\x1B[0m");
      stdout.write("\n");
    };

    const onKeypress = (_input: string, key: { name?: string; ctrl?: boolean }): void => {
      let keyAction: MultiSelectKey | null = null;
      if (key.ctrl && key.name === "c") {
        keyAction = "cancel";
      } else if (key.name === "up") {
        keyAction = "up";
      } else if (key.name === "down") {
        keyAction = "down";
      } else if (key.name === "space") {
        keyAction = "space";
      } else if (key.name === "return") {
        keyAction = "enter";
      } else if (key.name === "a") {
        keyAction = "toggle_all";
      }
      if (!keyAction) {
        return;
      }

      const reduced = reduceMultiSelectState(state, keyAction);
      state = reduced.state;
      if (reduced.action === "cancel") {
        cleanup();
        reject(new Error(options.cancelMessage));
        return;
      }
      if (reduced.action === "submit") {
        if (!hasAnySelected(state)) {
          stdout.write("\x07");
        } else {
          const selected: T[] = [];
          for (let index = 0; index < state.selected.length; index += 1) {
            if (state.selected[index]) {
              const opt = options.options[index];
              if (opt) {
                selected.push(opt.id);
              }
            }
          }
          cleanup();
          resolve(selected);
          return;
        }
      }
      redraw();
    };

    stdin.on("keypress", onKeypress);
  });
}
