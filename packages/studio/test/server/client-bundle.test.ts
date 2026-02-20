import { afterEach, describe, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { clientBundleIndexPath, shouldRebuildClientBundle } from "../../src/server/client-bundle";

const tempDirs: string[] = [];

afterEach(() => {
  for (const dir of tempDirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

interface StudioFixture {
  studioRoot: string;
  srcEntryPath: string;
  lockfilePath: string;
  inputPaths: string[];
}

function createFixture(): StudioFixture {
  const workspaceRoot = mkdtempSync(join(tmpdir(), "studio-client-bundle-"));
  tempDirs.push(workspaceRoot);
  const studioRoot = join(workspaceRoot, "packages/studio");

  mkdirSync(join(studioRoot, "src/client"), { recursive: true });
  writeFileSync(join(studioRoot, "src/client/main.tsx"), "export const ready = true;\n");
  writeFileSync(join(studioRoot, "src/index.html"), "<!doctype html>\n");
  writeFileSync(join(studioRoot, "src/vite.config.ts"), "export default {};\n");
  writeFileSync(join(studioRoot, "package.json"), "{}\n");
  writeFileSync(join(workspaceRoot, "bun.lock"), "# lockfile\n");

  const srcEntryPath = join(studioRoot, "src/client/main.tsx");
  const indexPath = join(studioRoot, "src/index.html");
  const viteConfigPath = join(studioRoot, "src/vite.config.ts");
  const packageJsonPath = join(studioRoot, "package.json");
  const lockfilePath = join(workspaceRoot, "bun.lock");

  return {
    studioRoot,
    srcEntryPath,
    lockfilePath,
    inputPaths: [srcEntryPath, indexPath, viteConfigPath, packageJsonPath, lockfilePath],
  };
}

function setMtimeMs(path: string, timeMs: number): void {
  const seconds = timeMs / 1000;
  utimesSync(path, seconds, seconds);
}

function setInputMtimeMs(fixture: StudioFixture, timeMs: number): void {
  for (const path of fixture.inputPaths) {
    setMtimeMs(path, timeMs);
  }
}

describe("studio client bundle freshness", () => {
  test("rebuilds when bundle does not exist", () => {
    const fixture = createFixture();
    expect(shouldRebuildClientBundle(fixture.studioRoot)).toBe(true);
  });

  test("rebuilds when source files are newer than built index", () => {
    const fixture = createFixture();
    const bundlePath = clientBundleIndexPath(fixture.studioRoot);

    mkdirSync(join(fixture.studioRoot, "dist/client"), { recursive: true });
    writeFileSync(bundlePath, "<!doctype html>\n");
    setInputMtimeMs(fixture, 1_000);
    setMtimeMs(bundlePath, 1_000);
    setMtimeMs(fixture.srcEntryPath, 2_000);

    expect(shouldRebuildClientBundle(fixture.studioRoot)).toBe(true);
  });

  test("does not rebuild when bundle is newer than sources", () => {
    const fixture = createFixture();
    const bundlePath = clientBundleIndexPath(fixture.studioRoot);

    mkdirSync(join(fixture.studioRoot, "dist/client"), { recursive: true });
    writeFileSync(bundlePath, "<!doctype html>\n");
    setInputMtimeMs(fixture, 1_000);
    setMtimeMs(bundlePath, 2_000);

    expect(shouldRebuildClientBundle(fixture.studioRoot)).toBe(false);
  });

  test("rebuilds when lockfile changes after bundle build", () => {
    const fixture = createFixture();
    const bundlePath = clientBundleIndexPath(fixture.studioRoot);

    mkdirSync(join(fixture.studioRoot, "dist/client"), { recursive: true });
    writeFileSync(bundlePath, "<!doctype html>\n");
    setInputMtimeMs(fixture, 1_000);
    setMtimeMs(bundlePath, 1_000);
    setMtimeMs(fixture.lockfilePath, 2_000);

    expect(shouldRebuildClientBundle(fixture.studioRoot)).toBe(true);
  });
});
