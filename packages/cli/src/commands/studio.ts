import { parseStudioPortArg, startStudioServer } from "@lore/studio";
import { parseArgs } from "node:util";
import { z } from "zod";

export const StudioArgsSchema = z.object({
  port: z.number().int().positive().optional(),
  open: z.boolean(),
});

export function parseStudioArgs(args: string[]): z.infer<typeof StudioArgsSchema> {
  const parsed = parseArgs({
    strict: true,
    args,
    allowPositionals: false,
    options: {
      port: { type: "string" },
      "no-open": { type: "boolean" },
    },
  });
  const portRaw = parsed.values.port;
  return StudioArgsSchema.parse({
    port: portRaw === undefined ? undefined : parseStudioPortArg(portRaw),
    open: !(parsed.values["no-open"] ?? false),
  });
}

export function runStudio(args: z.infer<typeof StudioArgsSchema>): void {
  const started = startStudioServer({
    port: args.port,
    open: args.open,
  });
  console.log(`Studio: ${started.url}`);
  console.log("Press Ctrl+C to stop.");
}
