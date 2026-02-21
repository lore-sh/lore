import { clearLine, clearScreenDown, cursorTo, emitKeypressEvents, moveCursor } from "node:readline";
import { stdin, stdout } from "node:process";
import { colorEnabled, style } from "./terminal";

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

function optionForCursor(cursor: ConfirmState["cursor"], options: readonly [ConfirmOption, ConfirmOption]): ConfirmOption {
  return options[cursor];
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
    if (option.hint && option.hint.trim().length > 0) {
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

export async function promptConfirm(options: ConfirmPromptOptions): Promise<boolean> {
  if (!stdin.isTTY || !stdout.isTTY) {
    throw new Error("interactive confirmation requires a TTY");
  }

  const confirmOptions: readonly [ConfirmOption, ConfirmOption] = [
    {
      value: true,
      label: options.yesLabel ?? "Yes",
      hint: options.yesHint,
    },
    {
      value: false,
      label: options.noLabel ?? "No",
      hint: options.noHint,
    },
  ];

  let state = createConfirmState(options.defaultValue ?? true);
  let renderedLines = 0;
  const withColor = colorEnabled();

  const redraw = (): void => {
    if (renderedLines > 0) {
      if (renderedLines > 1) {
        moveCursor(stdout, 0, -(renderedLines - 1));
      }
      cursorTo(stdout, 0);
    }

    const lines = renderConfirmPrompt(state, confirmOptions, withColor, options.title, options.message).split("\n");
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

  return await new Promise<boolean>((resolve, reject) => {
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
      let action: ConfirmKey | null = null;
      if (key.ctrl && key.name === "c") {
        action = "cancel";
      } else if (key.name === "up") {
        action = "up";
      } else if (key.name === "down") {
        action = "down";
      } else if (key.name === "left") {
        action = "left";
      } else if (key.name === "right") {
        action = "right";
      } else if (key.name === "space") {
        action = "toggle";
      } else if (key.name === "return") {
        action = "enter";
      } else if (key.name === "y") {
        action = "yes";
      } else if (key.name === "n") {
        action = "no";
      }

      if (!action) {
        return;
      }

      const reduced = reduceConfirmState(state, action);
      state = reduced.state;

      if (reduced.action === "cancel") {
        cleanup();
        reject(new Error(options.cancelMessage ?? "prompt cancelled"));
        return;
      }

      if (reduced.action === "submit") {
        const selected = optionForCursor(state.cursor, confirmOptions);
        cleanup();
        resolve(selected.value);
        return;
      }

      redraw();
    };

    stdin.on("keypress", onKeypress);
  });
}
