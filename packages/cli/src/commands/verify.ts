import { verify, type Database } from "@toss/core";
import { parseArgs } from "node:util";
import { z } from "zod";
import { toJson } from "../format";

export const VerifyArgsSchema = z.object({ full: z.boolean() });

export function parseVerifyArgs(args: string[]): z.infer<typeof VerifyArgsSchema> {
  const parsed = parseArgs({
    strict: true,
    args,
    allowPositionals: false,
    options: {
      full: { type: "boolean" },
    },
  });
  return VerifyArgsSchema.parse({ full: parsed.values.full ?? false });
}

export function runVerify(db: Database, args: z.infer<typeof VerifyArgsSchema>): void {
  const { full } = args;
  const result = verify(db, { full });
  console.log(toJson(result));
  if (!result.ok) {
    process.exit(1);
  }
}
