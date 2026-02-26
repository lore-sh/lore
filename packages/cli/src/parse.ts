import { parseArgs, type ParseArgsConfig } from "node:util";
import { z } from "zod";

interface CliParseOptions {
  allowPositionals?: boolean;
  options?: ParseArgsConfig["options"];
}

export function parseCliArgs(args: string[], config: CliParseOptions = {}) {
  return parseArgs({
    strict: true,
    args,
    allowPositionals: config.allowPositionals ?? false,
    options: config.options ?? {},
  });
}

export function parseNoArgs(args: string[], command: string): void {
  if (args.length > 0) {
    throw new Error(`${command} does not accept arguments`);
  }
}

export function parseRequiredPositional(args: string[]): string {
  const parsed = parseCliArgs(args, { allowPositionals: true });
  const [value] = z.tuple([z.string().trim().min(1)]).parse(parsed.positionals);
  return value;
}

export function parseOptionalPositional(args: string[]): string | undefined {
  const parsed = parseCliArgs(args, { allowPositionals: true });
  const positionals = z.array(z.string()).max(1).parse(parsed.positionals);
  return positionals[0];
}
