import { getStudioSchema } from "@toss/core";
import { Hono } from "hono";

export function createSchemaRoutes() {
  return new Hono().get("/api/schema", (c) => {
    return c.json(getStudioSchema());
  });
}
