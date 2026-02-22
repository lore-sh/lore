import { revertCommit } from "@toss/core";
import { Hono } from "hono";

export function createRevertRoutes() {
  return new Hono().post("/api/revert/:id", (c) => {
    return c.json(revertCommit(c.req.param("id")));
  });
}
