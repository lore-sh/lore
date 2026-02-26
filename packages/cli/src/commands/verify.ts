import { verify, type Database } from "@lore/core";
import { z } from "zod";
import { toJson } from "../format";
import { parseCliArgs } from "../parse";

export const VerifyArgsSchema = z.object({ full: z.boolean() });

export function parseVerifyArgs(args: string[]): z.infer<typeof VerifyArgsSchema> {
  const parsed = parseCliArgs(args, {
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
