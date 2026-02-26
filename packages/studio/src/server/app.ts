import { CodedError, toHttpProblem, type Database } from "@lore/core";
import { Hono } from "hono";
import { HTTPException } from "hono/http-exception";
import { createHistoryRoutes } from "./routes/history";
import { createRevertRoutes } from "./routes/revert";
import { createStatusRoutes } from "./routes/status";
import { createTableRoutes } from "./routes/tables";

export interface StudioApiError {
  code: string;
  message: string;
  details?: unknown;
}

type StudioAssetMeta = Readonly<{ contentType: string; etag: string }>;

const studioAssetMetaByPath = new Map<string, StudioAssetMeta>();
let cachedStudioAssetPaths: ReadonlyMap<string, string> | null | undefined;
let studioAssetPathsForTest: ReadonlyMap<string, string> | null | undefined;

function sha256Hex(bytes: Uint8Array): string {
  const hasher = new Bun.CryptoHasher("sha256");
  hasher.update(bytes);
  return hasher.digest("hex");
}

async function loadStudioAssetMeta(assetPath: string): Promise<StudioAssetMeta> {
  const cached = studioAssetMetaByPath.get(assetPath);
  if (cached) {
    return cached;
  }
  const file = Bun.file(assetPath);
  const bytes = await file.bytes();
  const meta = {
    contentType: file.type || "application/octet-stream",
    etag: sha256Hex(bytes),
  };
  studioAssetMetaByPath.set(assetPath, meta);
  return meta;
}

function readStudioAssetPathsFromModule(value: unknown): ReadonlyMap<string, string> | null {
  if (!value || typeof value !== "object") {
    return null;
  }
  const paths = Reflect.get(value, "STUDIO_ASSET_PATHS");
  if (!(paths instanceof Map)) {
    return null;
  }
  for (const [key, path] of paths) {
    if (typeof key !== "string" || typeof path !== "string") {
      return null;
    }
  }
  return paths;
}

async function loadStudioAssetPaths(): Promise<ReadonlyMap<string, string> | null> {
  if (studioAssetPathsForTest !== undefined) {
    return studioAssetPathsForTest;
  }
  if (cachedStudioAssetPaths !== undefined) {
    return cachedStudioAssetPaths;
  }
  try {
    const embeddedAssetsModulePath = "./generated/embedded-assets";
    const mod = await import(embeddedAssetsModulePath);
    cachedStudioAssetPaths = readStudioAssetPathsFromModule(mod);
  } catch {
    cachedStudioAssetPaths = null;
  }
  return cachedStudioAssetPaths;
}

export function setStudioAssetPathsForTest(paths: ReadonlyMap<string, string> | null | undefined): void {
  studioAssetPathsForTest = paths;
  studioAssetMetaByPath.clear();
}

function normalizeAssetRequestPath(path: string): string | null {
  const relative = path.replace(/^\/+/, "");
  if (relative.includes("..")) {
    return null;
  }
  if (relative.length === 0) {
    return "/";
  }
  return `/${relative}`;
}

async function buildAssetResponse(path: string, request: Request): Promise<Response | null> {
  const studioAssetPaths = await loadStudioAssetPaths();
  if (!studioAssetPaths || studioAssetPaths.size === 0) {
    return null;
  }
  const normalizedPath = normalizeAssetRequestPath(path);
  if (!normalizedPath) {
    return null;
  }
  const assetPath = studioAssetPaths.get(normalizedPath);
  if (!assetPath) {
    return null;
  }
  const file = Bun.file(assetPath);
  if (!(await file.exists())) {
    return null;
  }
  const meta = await loadStudioAssetMeta(assetPath);

  const requestEtag = request.headers.get("if-none-match");
  if (requestEtag && requestEtag === meta.etag) {
    return new Response(null, {
      status: 304,
      headers: {
        etag: meta.etag,
      },
    });
  }

  return new Response(file, {
    headers: {
      "content-type": meta.contentType,
      etag: meta.etag,
    },
  });
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
      const exactAsset = await buildAssetResponse(c.req.path, c.req.raw);
      if (exactAsset) {
        return exactAsset;
      }
      if (isAssetRequestPath(c.req.path)) {
        return c.text("Asset not found", 404);
      }

      const indexAsset = await buildAssetResponse("/", c.req.raw);
      if (indexAsset) {
        return indexAsset;
      }

      return c.text("Studio assets are not available. Run the Vite dev server for development.", 500);
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
