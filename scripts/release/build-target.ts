#!/usr/bin/env bun
import { chmodSync, mkdirSync, statSync } from "node:fs";
import { join, resolve } from "node:path";

const REPO_ROOT = resolve(import.meta.dir, "../..");
const STUDIO_ROOT = join(REPO_ROOT, "packages/studio");
const CLI_ENTRYPOINT = join(REPO_ROOT, "packages/cli/src/main.ts");
const GENERATE_EMBEDDED_MODULES_SCRIPT = join(REPO_ROOT, "scripts/release/gen-embedded-modules.ts");

interface BuildTargetOptions {
  version: string;
  target: Bun.Build.CompileTarget;
  osName: string;
  archName: string;
  outDir: string;
  debug: boolean;
}

function usage(): never {
  throw new Error("Usage: build-target.ts --version <semver> --target <bun-target> --os <linux|darwin> --arch <x64|arm64> --out-dir <dir> [--debug]");
}

function nextArg(argv: ReadonlyArray<string | undefined>, index: number): string {
  const value = argv[index];
  if (typeof value !== "string" || value.length === 0) {
    usage();
  }
  return value;
}

function parseArgs(argv: ReadonlyArray<string | undefined>): BuildTargetOptions {
  let version = "";
  let targetRaw = "";
  let osName = "";
  let archName = "";
  let outDir = "";
  let debug = false;

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (typeof arg !== "string") {
      usage();
    }
    if (arg === "--version") {
      version = nextArg(argv, i + 1);
      i += 1;
      continue;
    }
    if (arg === "--target") {
      targetRaw = nextArg(argv, i + 1);
      i += 1;
      continue;
    }
    if (arg === "--os") {
      osName = nextArg(argv, i + 1);
      i += 1;
      continue;
    }
    if (arg === "--arch") {
      archName = nextArg(argv, i + 1);
      i += 1;
      continue;
    }
    if (arg === "--out-dir") {
      outDir = nextArg(argv, i + 1);
      i += 1;
      continue;
    }
    if (arg === "--debug") {
      debug = true;
      continue;
    }
    usage();
  }

  if (!version || !targetRaw || !osName || !archName || !outDir) {
    usage();
  }

  const semverPattern = /^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$/;
  if (!semverPattern.test(version)) {
    throw new Error(`Invalid version: ${version}`);
  }
  if (!isCompileTarget(targetRaw)) {
    throw new Error(`Invalid target: ${targetRaw}`);
  }

  return {
    version,
    target: targetRaw,
    osName,
    archName,
    outDir,
    debug,
  };
}

function isCompileTarget(value: string): value is Bun.Build.CompileTarget {
  return /^bun-(darwin|linux|windows)-/.test(value);
}

function runCommand(args: string[], errorMessage: string): void {
  const result = Bun.spawnSync(args, {
    stdout: "pipe",
    stderr: "pipe",
    stdin: "ignore",
  });
  if (result.stdout.length > 0) {
    process.stderr.write(result.stdout);
  }
  if (result.stderr.length > 0) {
    process.stderr.write(result.stderr);
  }
  if (result.exitCode !== 0) {
    throw new Error(errorMessage);
  }
}

function buildStudioClientBundle(): void {
  runCommand(["bun", "run", "--cwd", STUDIO_ROOT, "build:client"], "Failed to build studio client assets");
}

function generateEmbeddedModules(): void {
  runCommand(["bun", "run", GENERATE_EMBEDDED_MODULES_SCRIPT], "Failed to generate embedded modules");
}

function cleanupEmbeddedModules(): void {
  runCommand(["bun", "run", GENERATE_EMBEDDED_MODULES_SCRIPT, "--clean"], "Failed to clean embedded modules");
}

async function buildTarget(options: BuildTargetOptions): Promise<string> {
  let buildError: unknown | null = null;
  try {
    buildStudioClientBundle();
    cleanupEmbeddedModules();
    generateEmbeddedModules();

    mkdirSync(options.outDir, { recursive: true });
    const outfile = join(options.outDir, `lore_${options.version}_${options.osName}_${options.archName}`);
    const sourcemap: Bun.BuildConfig["sourcemap"] = options.debug ? "linked" : "none";
    const buildConfig = {
      entrypoints: [CLI_ENTRYPOINT],
      compile: {
        target: options.target,
        outfile,
      },
      env: "disable" as const,
      autoloadDotenv: false,
      autoloadBunfig: false,
      autoloadPackageJson: false,
      autoloadTsconfig: false,
      sourcemap,
      minify: true,
      bytecode: true,
      define: {
        LORE_BUILD_VERSION: JSON.stringify(options.version),
      },
    };
    const result = await Bun.build(buildConfig);

    if (!result.success) {
      const messages = result.logs.map((log) => log.message).join("\n");
      throw new Error(messages.length > 0 ? messages : "bun build failed");
    }

    const output = statSync(outfile, { throwIfNoEntry: false });
    if (!output?.isFile()) {
      throw new Error(`bun build did not produce an output artifact at ${outfile}`);
    }
    chmodSync(outfile, 0o755);
    return outfile;
  } catch (error) {
    buildError = error;
    throw error;
  } finally {
    try {
      cleanupEmbeddedModules();
    } catch (cleanupError) {
      if (buildError !== null) {
        const message = cleanupError instanceof Error ? cleanupError.message : String(cleanupError);
        process.stderr.write(`warning: cleanup of embedded modules failed: ${message}\n`);
      } else {
        throw cleanupError;
      }
    }
  }
}

const options = parseArgs(Bun.argv.slice(2));
const outfile = await buildTarget(options);
console.log(outfile);
