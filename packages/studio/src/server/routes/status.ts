import { getStatus } from "@toss/core";
import { Hono } from "hono";
import type { StudioServerOptions } from "../types";

export function createStatusRoutes(options: StudioServerOptions) {
  return new Hono().get("/api/status", (c) => {
    return c.json(getStatus(options));
  });
}
