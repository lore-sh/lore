import { getStatus } from "@toss/core";
import { Hono } from "hono";

export function createStatusRoutes() {
  return new Hono().get("/api/status", (c) => {
    return c.json(getStatus());
  });
}
