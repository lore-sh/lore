import { getStudioCommitDetail, listStudioHistory } from "@toss/core";
import { Hono } from "hono";
import { validator } from "hono/validator";
import type { StudioServerOptions } from "../types";

function parsePositiveInt(value: string | undefined): number | undefined {
  if (!value) {
    return undefined;
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed < 1) {
    return undefined;
  }
  return parsed;
}

function singleValue(input: string | string[] | undefined): string | undefined {
  if (typeof input === "string") {
    return input;
  }
  if (Array.isArray(input)) {
    return typeof input[0] === "string" ? input[0] : undefined;
  }
  return undefined;
}

export function createHistoryRoutes(options: StudioServerOptions) {
  const dbOptions = options.dbPath ? { dbPath: options.dbPath } : {};
  const historyQuery = validator("query", (query) => ({
    limit: parsePositiveInt(singleValue(query.limit)),
  }));

  return new Hono()
    .get("/api/history", historyQuery, (c) => {
      const query = c.req.valid("query");
      return c.json(
        listStudioHistory({
          ...dbOptions,
          limit: query.limit,
        }),
      );
    })
    .get("/api/history/:id", (c) => {
      return c.json(getStudioCommitDetail(c.req.param("id"), dbOptions));
    });
}
