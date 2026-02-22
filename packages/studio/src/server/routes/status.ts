import { getStatus } from "@toss/core";
import { Hono } from "hono";

export function createStatusRoutes() {
  return new Hono().get("/status", (c) => {
    return c.json(getStatus(), 200);
  });
}
