import { zValidator } from "@hono/zod-validator";
import type { Database } from "bun:sqlite";
import { getStudioCommitDetail, listStudioHistory } from "@toss/core";
import { Hono } from "hono";
import { z } from "zod";
import { commitIdParamSchema, positiveIntSchema, validationError } from "./shared";

const commitsQuerySchema = z.object({
  limit: positiveIntSchema.optional(),
  page: positiveIntSchema.optional(),
  kind: z.enum(["apply", "revert"]).optional(),
  table: z.string().trim().min(1).optional(),
});

export function createHistoryRoutes(db: Database) {
  return new Hono()
    .get(
      "/",
      zValidator("query", commitsQuerySchema, (result, c) => {
        if (!result.success) {
          return c.json(validationError(result.error.issues), 400);
        }
      }),
      (c) => {
        const query = c.req.valid("query");

        return c.json(
          listStudioHistory(db, {
            limit: query.limit,
            page: query.page,
            kind: query.kind,
            table: query.table,
          }),
          200,
        );
      },
    )
    .get(
      "/:id",
      zValidator("param", commitIdParamSchema, (result, c) => {
        if (!result.success) {
          return c.json(validationError(result.error.issues), 400);
        }
      }),
      (c) => {
        const param = c.req.valid("param");
        return c.json(getStudioCommitDetail(db, param.id), 200);
      },
    );
}
