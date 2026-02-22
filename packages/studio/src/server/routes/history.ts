import { zValidator } from "@hono/zod-validator";
import { getStudioCommitDetail, listStudioHistory } from "@toss/core";
import { Hono } from "hono";
import { z } from "zod";

const positiveIntSchema = z.coerce.number().int().min(1);

const commitIdParamSchema = z.object({
  id: z.string().trim().min(1),
});

const commitsQuerySchema = z.object({
  limit: positiveIntSchema.optional(),
  page: positiveIntSchema.optional(),
  kind: z.enum(["apply", "revert"]).optional(),
  table: z.string().trim().min(1).optional(),
});

function validationError(issues: z.ZodIssue[]): { code: string; message: string; details: z.ZodIssue[] } {
  return {
    code: "VALIDATION_ERROR",
    message: "Request validation failed",
    details: issues,
  };
}

export function createHistoryRoutes() {
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
          listStudioHistory({
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
        return c.json(getStudioCommitDetail(param.id), 200);
      },
    );
}
