import { clearLine, clearScreenDown, cursorTo, emitKeypressEvents, moveCursor } from "node:readline";
import { stdin, stdout } from "node:process";

type KeyEvent = { name?: string; ctrl?: boolean };
type KeyResult<T> = { action: "none" } | { action: "submit"; value: T } | { action: "cancel" };

export interface PromptConfig<T> {
  render(): string;
  onKey(key: KeyEvent): KeyResult<T> | null;
  cancelMessage: string;
}

export function runPrompt<T>(config: PromptConfig<T>): Promise<T> {
  if (!stdin.isTTY || !stdout.isTTY) {
    throw new Error("interactive selection requires a TTY");
  }

  let renderedLines = 0;

  function redraw(): void {
    if (renderedLines > 0) {
      if (renderedLines > 1) {
        moveCursor(stdout, 0, -(renderedLines - 1));
      }
      cursorTo(stdout, 0);
    }
    const lines = config.render().split("\n");
    for (let i = 0; i < lines.length; i += 1) {
      clearLine(stdout, 0);
      cursorTo(stdout, 0);
      stdout.write(lines[i] ?? "");
      if (i < lines.length - 1) {
        stdout.write("\n");
      }
    }
    renderedLines = lines.length;
    clearScreenDown(stdout);
  }

  emitKeypressEvents(stdin);
  stdin.setRawMode(true);
  stdin.resume();
  stdout.write("\x1B[?25l");
  stdout.write("\n");
  redraw();

  return new Promise<T>((resolve, reject) => {
    function cleanup(): void {
      stdin.off("keypress", onKeypress);
      if (stdin.isRaw) {
        stdin.setRawMode(false);
      }
      stdin.pause();
      stdout.write("\x1B[?25h");
      stdout.write("\x1B[0m");
      stdout.write("\n");
    }

    function onKeypress(_input: string, key: KeyEvent): void {
      const result = config.onKey(key);
      if (!result) return;
      if (result.action === "cancel") {
        cleanup();
        reject(new Error(config.cancelMessage));
        return;
      }
      if (result.action === "submit") {
        cleanup();
        resolve(result.value);
        return;
      }
      redraw();
    }

    stdin.on("keypress", onKeypress);
  });
}
