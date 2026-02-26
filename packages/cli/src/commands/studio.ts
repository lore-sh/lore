import { parseStudioPortArg, startStudioServer } from "@lore/studio";
import { z } from "zod";
import { parseCliArgs } from "../parse";

export const StudioArgsSchema = z.object({
  port: z.number().int().positive().optional(),
  open: z.boolean(),
});

export function parseStudioArgs(args: string[]): z.infer<typeof StudioArgsSchema> {
  const parsed = parseCliArgs(args, {
    options: {
      port: { type: "string" },
      "no-open": { type: "boolean" },
    },
  });
  const portRawValue = parsed.values.port;
  const portRaw = portRawValue === undefined ? undefined : z.string().parse(portRawValue);
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
