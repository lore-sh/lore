import { type Database } from "@toss/core";
import { z } from "zod";
import {
  parseRemoteConnectArgs,
  ParsedRemoteConnectArgsSchema,
  runRemoteConnect,
} from "./remote-connect";
import { parseRemoteStatusArgs, runRemoteStatus } from "./remote-status";

const RemoteSubcommandSchema = z.enum(["connect", "status"]);
export const RemoteArgsSchema = z.discriminatedUnion("sub", [
  z.object({ sub: z.literal("connect"), args: ParsedRemoteConnectArgsSchema }),
  z.object({ sub: z.literal("status") }),
]);

export function parseRemoteArgs(args: string[]): z.infer<typeof RemoteArgsSchema> {
  const sub = args[0];
  if (!sub) {
    throw new Error("remote requires subcommand: connect | status");
  }
  const rest = args.slice(1);
  const parsedSub = RemoteSubcommandSchema.parse(sub);
  if (parsedSub === "connect") {
    return RemoteArgsSchema.parse({
      sub: "connect",
      args: parseRemoteConnectArgs(rest),
    });
  }
  parseRemoteStatusArgs(rest);
  return RemoteArgsSchema.parse({ sub: "status" });
}

export async function runRemote(db: Database, args: z.infer<typeof RemoteArgsSchema>): Promise<void> {
  if (args.sub === "connect") {
    await runRemoteConnect(db, args.args);
    return;
  }
  await runRemoteStatus(db);
}
