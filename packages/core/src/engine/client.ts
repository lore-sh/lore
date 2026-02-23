import { Database } from "bun:sqlite";
import { drizzle } from "drizzle-orm/bun-sqlite";
import * as schema from "./schema.sql";

export function createEngineDb(sqlite: Database) {
  return drizzle({ client: sqlite, schema });
}
