import type { Database } from "bun:sqlite";
import { status } from "@toss/core";
import { Hono } from "hono";

export function createStatusRoutes(db: Database) {
  return new Hono().get("/status", (c) => {
    return c.json(status(db), 200);
  });
}
