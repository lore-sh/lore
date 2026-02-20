import { existsSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";

const CLIENT_BUNDLE_INDEX_RELATIVE_PATH = "dist/client/index.html";

function bundleInputRoots(studioRoot: string): string[] {
  return [
    join(studioRoot, "src/client"),
    join(studioRoot, "src/index.html"),
    join(studioRoot, "src/vite.config.ts"),
    join(studioRoot, "package.json"),
    join(studioRoot, "../../bun.lock"),
  ];
}

function collectFiles(path: string): string[] {
  if (!existsSync(path)) {
    return [];
  }

  const stat = statSync(path);
  if (stat.isFile()) {
    return [path];
  }
  if (!stat.isDirectory()) {
    return [];
  }

  const files: string[] = [];
  for (const entry of readdirSync(path, { withFileTypes: true })) {
    if (entry.isDirectory()) {
      files.push(...collectFiles(join(path, entry.name)));
      continue;
    }
    if (entry.isFile()) {
      files.push(join(path, entry.name));
    }
  }
  return files;
}

function latestInputMtimeMs(studioRoot: string): number {
  let latest = 0;
  for (const inputRoot of bundleInputRoots(studioRoot)) {
    for (const file of collectFiles(inputRoot)) {
      const mtime = statSync(file).mtimeMs;
      if (mtime > latest) {
        latest = mtime;
      }
    }
  }
  return latest;
}

export function clientBundleIndexPath(studioRoot: string): string {
  return join(studioRoot, CLIENT_BUNDLE_INDEX_RELATIVE_PATH);
}

export function shouldRebuildClientBundle(studioRoot: string): boolean {
  const bundlePath = clientBundleIndexPath(studioRoot);
  if (!existsSync(bundlePath)) {
    return true;
  }

  const latestInputMtime = latestInputMtimeMs(studioRoot);
  if (latestInputMtime === 0) {
    return false;
  }

  const bundleMtime = statSync(bundlePath).mtimeMs;
  return latestInputMtime > bundleMtime;
}
