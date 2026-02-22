import { getStudioTableSchema, listStudioTableHistory, listStudioTables, readStudioTable } from "@toss/core";
import { Hono } from "hono";
import { validator } from "hono/validator";
import { parsePositiveInt, singleValue } from "./query";

function parseFilters(query: Record<string, string | string[] | undefined>): Record<string, string> {
  const filters: Record<string, string> = {};
  for (const [key, value] of Object.entries(query)) {
    if (!key.startsWith("filters.")) {
      continue;
    }
    const column = key.slice("filters.".length).trim();
    if (column.length === 0) {
      continue;
    }
    const raw = singleValue(value);
    if (!raw) {
      continue;
    }
    const normalized = raw.trim();
    if (normalized.length === 0) {
      continue;
    }
    filters[column] = normalized;
  }
  return filters;
}

export function createTableRoutes() {
  const tableQuery = validator("query", (query) => {
    const page = parsePositiveInt(singleValue(query.page));
    const pageSize = parsePositiveInt(singleValue(query.pageSize));
    const sortBy = singleValue(query.sortBy);
    const sortDir: "asc" | "desc" = singleValue(query.sortDir) === "desc" ? "desc" : "asc";
    return {
      page,
      pageSize,
      sortBy: typeof sortBy === "string" && sortBy.trim().length > 0 ? sortBy : undefined,
      sortDir,
      filters: parseFilters(query),
    };
  });
  const historyQuery = validator("query", (query) => ({
    limit: parsePositiveInt(singleValue(query.limit)),
  }));

  return new Hono()
    .get("/api/tables", (c) => {
      return c.json(listStudioTables());
    })
    .get("/api/tables/:name", tableQuery, (c) => {
      const query = c.req.valid("query");
      return c.json(
        readStudioTable({
          table: c.req.param("name"),
          page: query.page,
          pageSize: query.pageSize,
          sortBy: query.sortBy,
          sortDir: query.sortDir,
          filters: query.filters,
        }),
      );
    })
    .get("/api/tables/:name/schema", (c) => {
      return c.json(getStudioTableSchema(c.req.param("name")));
    })
    .get("/api/tables/:name/history", historyQuery, (c) => {
      const query = c.req.valid("query");
      return c.json(
        listStudioTableHistory(c.req.param("name"), {
          limit: query.limit,
        }),
      );
    });
}
