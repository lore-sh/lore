import { defineConfig } from "drizzle-kit";
import { homedir } from "node:os";
import { resolve } from "node:path";

const url = resolve(homedir(), ".toss", "toss.db");

export default defineConfig({
  dialect: "sqlite",
  schema: "./src/engine/schema.sql.ts",
  out: "./migration",
  dbCredentials: {
    url,
  },
});
