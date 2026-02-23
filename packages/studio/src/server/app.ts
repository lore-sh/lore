import { CodedError, toHttpProblem, type Database } from "@toss/core";
import { Hono } from "hono";
import { HTTPException } from "hono/http-exception";
import { join } from "node:path";
import { createHistoryRoutes } from "./routes/history";
import { createRevertRoutes } from "./routes/revert";
import { createStatusRoutes } from "./routes/status";
import { createTableRoutes } from "./routes/tables";

export interface StudioApiError {
  code: string;
  message: string;
  details?: unknown;
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

function jsonError(code: string, message: string, status: number, details?: unknown): Response {
  const payload: StudioApiError = details !== undefined
    ? { code, message, details }
    : { code, message };

  return new Response(JSON.stringify(payload), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

function jsonProblem(problem: ReturnType<typeof toHttpProblem>): Response {
  return new Response(JSON.stringify(problem), {
    status: problem.status,
    headers: { "content-type": "application/problem+json; charset=utf-8" },
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

export function createStudioApi(db: Database) {
  return new Hono()
    .basePath("/api")
    .route("/tables", createTableRoutes(db))
    .route("/commits", createHistoryRoutes(db))
    .route("/commits", createRevertRoutes(db))
    .route("/", createStatusRoutes(db));
}

export function createStudioApp(db: Database) {
  const api = createStudioApi(db);
  const app = new Hono()
    .route("/", api)
    .all("/api/*", (c) => {
      return c.json(
        {
          code: "NOT_FOUND",
          message: "API route not found",
        } satisfies StudioApiError,
        404,
      );
    })
    .get("*", async (c) => {
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

  app.onError((error, c) => {
    if (CodedError.is(error)) {
      return jsonProblem(toHttpProblem(error, c.req.path));
    }

    if (error instanceof HTTPException) {
      const message = error.message || "HTTP exception";
      if (error.status === 400 && message.toLowerCase().includes("malformed json")) {
        return jsonError("VALIDATION_ERROR", "Request validation failed", 400, [
          {
            message,
          },
        ]);
      }
      if (error.status >= 400 && error.status < 500) {
        return jsonError("INVALID_OPERATION", message, error.status);
      }
      return jsonError("INTERNAL", message, 500);
    }

    if (error instanceof SyntaxError) {
      return jsonError("VALIDATION_ERROR", "Request validation failed", 400, [
        {
          message: error.message,
        },
      ]);
    }

    const message = error instanceof Error ? error.message : String(error);
    return jsonError("INTERNAL", message, 500);
  });

  return app;
}
export type StudioApi = ReturnType<typeof createStudioApi>;
