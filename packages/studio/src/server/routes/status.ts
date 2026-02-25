import { status, type Database } from "@lore/core";
import { Hono } from "hono";

export function createStatusRoutes(db: Database) {
  return new Hono().get("/status", (c) => {
    return c.json(status(db), 200);
  });
}
