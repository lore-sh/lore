import { getStudioSchema } from "@toss/core";
import { Hono } from "hono";
import type { StudioServerOptions } from "../types";

export function createSchemaRoutes(options: StudioServerOptions) {
  return new Hono().get("/api/schema", (c) => {
    return c.json(getStudioSchema(options));
  });
}
