import { zValidator } from "@hono/zod-validator";
import type { Database } from "bun:sqlite";
import { revert } from "@toss/core";
import { Hono } from "hono";
import { commitIdParamSchema, validationError } from "./shared";

export function createRevertRoutes(db: Database) {
  return new Hono().post(
    "/:id/revert",
    zValidator("param", commitIdParamSchema, (result, c) => {
      if (!result.success) {
        return c.json(validationError(result.error.issues), 400);
      }
    }),
    (c) => {
      const param = c.req.valid("param");
      const result = revert(db, param.id);
      if (result.ok) {
        return c.json(result, 200);
      }
      return c.json(result, 409);
    },
  );
}
