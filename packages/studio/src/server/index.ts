import { createStudioApp } from "./app";
import { join } from "node:path";
import { shouldRebuildClientBundle } from "./client-bundle";
import { DEFAULT_STUDIO_PORT, normalizeStudioPort, parseStudioPortArg } from "./port";
import type { StartStudioServerOptions } from "./types";

const DEFAULT_HOST = "127.0.0.1";

function parseStudioArgs(args: string[]): { port: number; open: boolean } {
  let port = DEFAULT_STUDIO_PORT;
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
    throw new Error(`Unknown studio argument: ${arg}`);
  }

  return { port, open };
}

function browserCommand(url: string): string[] {
  switch (process.platform) {
    case "darwin":
      return ["open", url];
    case "win32":
      return ["cmd", "/c", "start", "", url];
    default:
      return ["xdg-open", url];
  }
}

function openBrowser(url: string): void {
  try {
    Bun.spawn(browserCommand(url), {
      stdin: "ignore",
      stdout: "ignore",
      stderr: "ignore",
    });
  } catch {
    // ignore browser-open errors and keep server running
  }
}

function studioPackageRoot(): string {
  return join(import.meta.dir, "../..");
}

function ensureClientBundle(): void {
  const studioRoot = studioPackageRoot();
  if (!shouldRebuildClientBundle(studioRoot)) {
    return;
  }
  const result = Bun.spawnSync(["bun", "run", "--cwd", studioRoot, "build:client"], {
    stdout: "inherit",
    stderr: "inherit",
    stdin: "ignore",
  });
  if (result.exitCode !== 0) {
    throw new Error("Failed to build studio client assets");
  }
}

export interface StartedStudioServer {
  url: string;
  server: Bun.Server<unknown>;
}

export function startStudioServer(options: StartStudioServerOptions = {}): StartedStudioServer {
  ensureClientBundle();
  const port = normalizeStudioPort(options.port);
  const host = options.host ?? DEFAULT_HOST;
  const app = createStudioApp();
  const server = Bun.serve({
    hostname: host,
    port,
    fetch: app.fetch,
  });
  const url = `http://${host}:${server.port}`;
  if (options.open !== false) {
    openBrowser(url);
  }
  return { url, server };
}

if (import.meta.main) {
  const parsed = parseStudioArgs(Bun.argv.slice(2));
  const started = startStudioServer(parsed);
  console.log(`toss studio running at ${started.url}`);
}
