import { getStudioTableSchema, listStudioTables, readStudioTable, TossError } from "@toss/core";
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

function parseFilters(raw: string | undefined): Record<string, string> {
  if (!raw || raw.trim().length === 0) {
    return {};
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new TossError("INVALID_OPERATION", "Invalid filter query: expected JSON object");
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new TossError("INVALID_OPERATION", "Invalid filter query: expected JSON object");
  }
  const filters: Record<string, string> = {};
  for (const [key, value] of Object.entries(parsed)) {
    if (typeof value !== "string") {
      continue;
    }
    const column = key.trim();
    if (!column) {
      continue;
    }
    const normalized = value.trim();
    if (normalized.length === 0) {
      continue;
    }
    filters[column] = normalized;
  }
  return filters;
}

export function createTableRoutes(options: StudioServerOptions) {
  const dbOptions = options.dbPath ? { dbPath: options.dbPath } : {};
  const tableQuery = validator("query", (query) => {
    const page = parsePositiveInt(singleValue(query.page));
    const pageSize = parsePositiveInt(singleValue(query.pageSize));
    const sortBy = singleValue(query.sortBy);
    const sortDir: "asc" | "desc" = singleValue(query.sortDir) === "desc" ? "desc" : "asc";
    const filter = singleValue(query.filter);
    return {
      page,
      pageSize,
      sortBy: typeof sortBy === "string" && sortBy.trim().length > 0 ? sortBy : undefined,
      sortDir,
      filter,
    };
  });

  return new Hono()
    .get("/api/tables", (c) => {
      return c.json(listStudioTables(dbOptions));
    })
    .get("/api/tables/:name", tableQuery, (c) => {
      const query = c.req.valid("query");
      return c.json(
        readStudioTable({
          ...dbOptions,
          table: c.req.param("name"),
          page: query.page,
          pageSize: query.pageSize,
          sortBy: query.sortBy,
          sortDir: query.sortDir,
          filters: parseFilters(query.filter),
        }),
      );
    })
    .get("/api/tables/:name/schema", (c) => {
      return c.json(getStudioTableSchema(c.req.param("name"), dbOptions));
    });
}
