import { type Database, describeDb } from "@lore/core";
import { z } from "zod";
import { toJson } from "../format";
import { parseOptionalPositional } from "../parse";

export const SchemaArgsSchema = z.object({ table: z.string().optional() });

export function parseSchemaArgs(args: string[]): z.infer<typeof SchemaArgsSchema> {
  return SchemaArgsSchema.parse({ table: parseOptionalPositional(args) });
}

export function runSchema(db: Database, args: z.infer<typeof SchemaArgsSchema>): void {
  const { table } = args;
  console.log(toJson(describeDb(db, { table })));
}
