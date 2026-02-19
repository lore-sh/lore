function isEnoent(error: unknown): boolean {
  return Boolean(error && typeof error === "object" && Reflect.get(error, "code") === "ENOENT");
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
