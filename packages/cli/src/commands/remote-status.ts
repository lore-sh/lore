import { remoteStatus, type Database } from "@lore/core";
import { toJson } from "../format";
import { parseNoArgs } from "../parse";

export function parseRemoteStatusArgs(args: string[]): void {
  parseNoArgs(args, "remote status");
}

export async function runRemoteStatus(db: Database): Promise<void> {
  const currentStatus = await remoteStatus(db);
  console.log(toJson(currentStatus));
}
