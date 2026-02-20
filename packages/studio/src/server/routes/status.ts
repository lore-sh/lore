import { getStatus } from "@toss/core";
import { Hono } from "hono";
import type { StudioServerOptions } from "../types";

export function createStatusRoutes(options: StudioServerOptions) {
  const dbOptions = options.dbPath ? { dbPath: options.dbPath } : {};
  return new Hono().get("/api/status", (c) => {
    return c.json(getStatus(dbOptions));
  });
}
