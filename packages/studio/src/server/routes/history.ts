import { zValidator } from "@hono/zod-validator";
import {
  CodedError,
  history,
  findCommit,
  commitOperations,
  commitRowEffects,
  decodeRowEffects,
  commitSchemaEffects,
  type Database,
} from "@toss/core";
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
          history(db, {
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
        const commit = findCommit(db, param.id);
        if (!commit) {
          throw new CodedError("NOT_FOUND", `Commit not found: ${param.id}`);
        }
        const rowEffects = decodeRowEffects(commitRowEffects(db, param.id));
        return c.json(
          {
            commit,
            operations: commitOperations(db, param.id),
            effects: {
              rows: rowEffects,
              schemas: commitSchemaEffects(db, param.id),
            },
          },
          200,
        );
      },
    );
}
