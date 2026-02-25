import { clone } from "@lore/core";
import { parseArgs } from "node:util";
import { z } from "zod";
import { toJson } from "../format";
import { RemotePlatformSchema } from "./remote-connect";

export const CloneArgsSchema = z.object({
  platform: RemotePlatformSchema,
  url: z.string().trim().min(1),
  forceNew: z.boolean(),
});

export function parseClonePlatform(value: string): z.infer<typeof RemotePlatformSchema> {
  const parsed = RemotePlatformSchema.safeParse(value);
  if (!parsed.success) {
    throw new Error(`clone does not accept --platform=${value}. Use turso or libsql.`);
  }
  return parsed.data;
}

export function parseCloneArgs(args: string[]): z.infer<typeof CloneArgsSchema> {
  const parsed = parseArgs({
    strict: true,
    args,
    allowPositionals: true,
    options: {
      platform: { type: "string" },
      "force-new": { type: "boolean" },
    },
  });
  const [url] = z.tuple([z.string().trim().min(1)]).parse(parsed.positionals);
  const platform = z.string().trim().min(1).parse(parsed.values.platform);
  const forceNew = z.boolean().parse(parsed.values["force-new"] ?? false);
  return CloneArgsSchema.parse({
    platform: parseClonePlatform(platform),
    url,
    forceNew,
  });
}

export async function runClone(args: z.infer<typeof CloneArgsSchema>): Promise<void> {
  const result = await clone(args);
  console.log(toJson({ status: "ok", db_path: result.dbPath, sync: result.sync }));
}
