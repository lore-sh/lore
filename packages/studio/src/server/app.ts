import { isTossError } from "@toss/core";
import { Hono } from "hono";
import { join } from "node:path";
import { createHistoryRoutes } from "./routes/history";
import { createRevertRoutes } from "./routes/revert";
import { createStatusRoutes } from "./routes/status";
import { createTableRoutes } from "./routes/tables";

function statusFromTossCode(code: string): number {
  switch (code) {
    case "NOT_FOUND":
      return 404;
    case "NOT_INITIALIZED":
    case "CONFIG_ERROR":
    case "INVALID_OPERATION":
    case "INVALID_IDENTIFIER":
    case "REVERT_UNSUPPORTED":
      return 400;
    case "ALREADY_REVERTED":
      return 409;
    default:
      return 500;
  }
}

function toAssetPath(path: string): string {
  const relative = path.replace(/^\/+/, "");
  if (relative.includes("..")) {
    return "";
  }
  return relative.length === 0 ? "index.html" : relative;
}

function clientAssetRoot(): string {
  return join(import.meta.dir, "../../dist/client");
}

function jsonResponse(payload: Record<string, unknown>, status: number): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
    },
  });
}

async function loadAsset(path: string): Promise<Response | null> {
  const filePath = join(clientAssetRoot(), toAssetPath(path));
  const file = Bun.file(filePath);
  if (await file.exists()) {
    return new Response(file);
  }
  return null;
}

export function isAssetRequestPath(path: string): boolean {
  return path.startsWith("/assets/");
}

export function createStudioApi() {
  return new Hono()
    .route("/", createTableRoutes())
    .route("/", createHistoryRoutes())
    .route("/", createRevertRoutes())
    .route("/", createStatusRoutes());
}

export function createStudioApp() {
  const api = createStudioApi();
  const app = new Hono().route("/", api).get("*", async (c) => {
    if (c.req.path.startsWith("/api/")) {
      return c.json({ error: "NOT_FOUND", message: "API route not found" }, 404);
    }

    const exactAsset = await loadAsset(c.req.path);
    if (exactAsset) {
      return exactAsset;
    }
    if (isAssetRequestPath(c.req.path)) {
      return c.text("Asset not found", 404);
    }

    const indexAsset = await loadAsset("/index.html");
    if (indexAsset) {
      return indexAsset;
    }

    return c.text("Studio client bundle not found. Run `bun run --cwd packages/studio build:client` first.", 500);
  });

  app.onError((error) => {
    if (isTossError(error)) {
      return jsonResponse(
        {
          error: error.code,
          message: error.message,
        },
        statusFromTossCode(error.code),
      );
    }
    const message = error instanceof Error ? error.message : String(error);
    return jsonResponse(
      {
        error: "INTERNAL",
        message,
      },
      500,
    );
  });
  return app;
}

export const studioApi = createStudioApi();
export type StudioApi = typeof studioApi;
