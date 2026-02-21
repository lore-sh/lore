import { stdout } from "node:process";

export function colorEnabled(): boolean {
  return stdout.isTTY && process.env.NO_COLOR !== "1" && process.env.TERM !== "dumb";
}

export function style(text: string, code: string, enabled: boolean): string {
  if (!enabled) {
    return text;
  }
  return `\x1B[${code}m${text}\x1B[0m`;
}
