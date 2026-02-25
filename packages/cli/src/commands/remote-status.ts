import { remoteStatus, type Database } from "@lore/core";
import { z } from "zod";
import { toJson } from "../format";

export const RemoteStatusArgsSchema = z.array(z.string()).length(0, "remote status does not accept arguments");

export function parseRemoteStatusArgs(args: string[]): void {
  RemoteStatusArgsSchema.parse(args);
}

export async function runRemoteStatus(db: Database): Promise<void> {
  const currentStatus = await remoteStatus(db);
  console.log(toJson(currentStatus));
}
