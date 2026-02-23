import { zValidator } from "@hono/zod-validator";
import { getStudioTableSchema, listStudioTableHistory, listStudioTables, readStudioTable } from "@toss/core";
import { Hono } from "hono";
import { z } from "zod";
import { positiveIntSchema, tableParamSchema, validationError } from "./shared";

const tableRowsQuerySchema = z.object({
  page: positiveIntSchema.optional(),
  pageSize: positiveIntSchema.optional(),
  sortBy: z.string().trim().min(1).optional(),
  sortDir: z.enum(["asc", "desc"]).optional(),
  filters: z.record(z.string(), z.string()).optional(),
});

const tableHistoryQuerySchema = z.object({
  limit: positiveIntSchema.optional(),
  page: positiveIntSchema.optional(),
});

function normalizeFilters(filters: Record<string, string> | undefined): Record<string, string> {
  const normalized: Record<string, string> = {};
  if (!filters) {
    return normalized;
  }

  for (const [column, value] of Object.entries(filters)) {
    const key = column.trim();
    const text = value.trim();
    if (key.length === 0 || text.length === 0) {
      continue;
    }
    normalized[key] = text;
  }

  return normalized;
}

export function createTableRoutes() {
  return new Hono()
    .get("/", (c) => {
      return c.json(listStudioTables(), 200);
    })
    .post(
      "/:name/rows/query",
      zValidator("param", tableParamSchema, (result, c) => {
        if (!result.success) {
          return c.json(validationError(result.error.issues), 400);
        }
      }),
      zValidator("json", tableRowsQuerySchema, (result, c) => {
        if (!result.success) {
          return c.json(validationError(result.error.issues), 400);
        }
      }),
      (c) => {
        const param = c.req.valid("param");
        const query = c.req.valid("json");

        return c.json(
          readStudioTable({
            table: param.name,
            page: query.page,
            pageSize: query.pageSize,
            sortBy: query.sortBy,
            sortDir: query.sortDir,
            filters: normalizeFilters(query.filters),
          }),
          200,
        );
      },
    )
    .get(
      "/:name/schema",
      zValidator("param", tableParamSchema, (result, c) => {
        if (!result.success) {
          return c.json(validationError(result.error.issues), 400);
        }
      }),
      (c) => {
        const param = c.req.valid("param");
        return c.json(getStudioTableSchema(param.name), 200);
      },
    )
    .get(
      "/:name/history",
      zValidator("param", tableParamSchema, (result, c) => {
        if (!result.success) {
          return c.json(validationError(result.error.issues), 400);
        }
      }),
      zValidator("query", tableHistoryQuerySchema, (result, c) => {
        if (!result.success) {
          return c.json(validationError(result.error.issues), 400);
        }
      }),
      (c) => {
        const param = c.req.valid("param");
        const query = c.req.valid("query");

        return c.json(
          listStudioTableHistory(param.name, {
            limit: query.limit,
            page: query.page,
          }),
          200,
        );
      },
    );
}
