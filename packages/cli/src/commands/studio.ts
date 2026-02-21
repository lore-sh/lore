import { parseStudioPortArg, startStudioServer } from "@toss/studio";

export function parseStudioArgs(args: string[]): { port?: number | undefined; open: boolean } {
  let port: number | undefined;
  let open = true;
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i]!;
    if (arg === "--port") {
      port = parseStudioPortArg(args[i + 1]);
      i += 1;
      continue;
    }
    if (arg === "--no-open") {
      open = false;
      continue;
    }
    throw new Error(`studio does not accept argument: ${arg}`);
  }
  return { port, open };
}

export function runStudio(args: string[]): void {
  const parsed = parseStudioArgs(args);
  const started = startStudioServer({
    port: parsed.port,
    open: parsed.open,
  });
  console.log(`Studio: ${started.url}`);
  console.log("Press Ctrl+C to stop.");
}
