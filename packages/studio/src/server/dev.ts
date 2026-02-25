import { openDb } from "@lore/core";
import { join } from "node:path";
import { createStudioApp } from "./app";

const API_PORT = 7056;
const HOST = "127.0.0.1";

const db = openDb();
const app = createStudioApp(db);

const server = Bun.serve({
  hostname: HOST,
  port: API_PORT,
  fetch: app.fetch,
});

console.log(`API server at http://${HOST}:${server.port}`);

const studioRoot = join(import.meta.dir, "../..");
const configPath = join(studioRoot, "src/vite.config.ts");

const vite = Bun.spawn(
  ["bunx", "vite", "--config", configPath, "--open"],
  {
    env: { ...process.env, STUDIO_API_PORT: String(server.port) },
    stdout: "inherit",
    stderr: "inherit",
    cwd: studioRoot,
  },
);

let cleaned = false;

function cleanup() {
  if (cleaned) {
    return;
  }
  cleaned = true;
  vite.kill();
  server.stop();
  try {
    db.$client.close(false);
  } catch {
    // ignore close errors during teardown
  }
}

process.on("SIGINT", () => {
  cleanup();
  process.exit(130);
});
process.on("SIGTERM", () => {
  cleanup();
  process.exit(143);
});

const viteExitCode = await vite.exited;
cleanup();
if (viteExitCode !== 0) {
  process.exit(viteExitCode);
}
