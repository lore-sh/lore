import { zValidator } from "@hono/zod-validator";
import { revertCommit } from "@toss/core";
import { Hono } from "hono";
import { z } from "zod";

const commitIdParamSchema = z.object({
  id: z.string().trim().min(1),
});

function validationError(issues: z.ZodIssue[]): { code: string; message: string; details: z.ZodIssue[] } {
  return {
    code: "VALIDATION_ERROR",
    message: "Request validation failed",
    details: issues,
  };
}

export function createRevertRoutes() {
  return new Hono().post(
    "/:id/revert",
    zValidator("param", commitIdParamSchema, (result, c) => {
      if (!result.success) {
        return c.json(validationError(result.error.issues), 400);
      }
    }),
    (c) => {
      const param = c.req.valid("param");
      const result = revertCommit(param.id);
      if (result.ok) {
        return c.json(result, 200);
      }
      return c.json(result, 409);
    },
  );
}
