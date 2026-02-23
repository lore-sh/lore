import { resolve } from "node:path";
import { CodedError } from "../error";

export function isEnoent(error: unknown): boolean {
  return error instanceof Error && "code" in error && error.code === "ENOENT";
}

export function resolveHomeDir(): string {
  const home = process.env.HOME ?? process.env.USERPROFILE;
  if (!home) {
    throw new CodedError("CONFIG", "HOME (or USERPROFILE) is required to resolve the home directory.");
  }
  return resolve(home);
}

export async function deleteIfExists(path: string): Promise<void> {
  try {
    await Bun.file(path).delete();
  } catch (error) {
    if (isEnoent(error)) {
      return;
    }
    throw error;
  }
}

export async function deleteWithSidecars(path: string): Promise<void> {
  await Promise.all([deleteIfExists(path), deleteIfExists(`${path}-wal`), deleteIfExists(`${path}-shm`)]);
}

export async function deleteWalAndShm(path: string): Promise<void> {
  await Promise.all([deleteIfExists(`${path}-wal`), deleteIfExists(`${path}-shm`)]);
}
