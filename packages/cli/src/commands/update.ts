import { chmodSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
import { z } from "zod";
import { parseCliArgs } from "../parse";
import { VERSION } from "../version";

const DEFAULT_INSTALL_URL = "https://getlore.sh/install";

export const UpdateArgsSchema = z.object({
  version: z.string().trim().min(1).optional(),
});

export interface UpdateRuntime {
  installUrl?: string | undefined;
  env?: NodeJS.ProcessEnv | undefined;
  currentVersion?: string | undefined;
  execPath?: string | undefined;
  fetchInstallScript?: ((url: string) => Promise<string>) | undefined;
  runInstallerScript?: ((script: string, env: NodeJS.ProcessEnv) => Promise<void>) | undefined;
}

export function parseUpdateArgs(args: string[]): z.infer<typeof UpdateArgsSchema> {
  const parsed = parseCliArgs(args, {
    options: {
      version: { type: "string" },
    },
  });
  const versionRaw = parsed.values.version;
  return UpdateArgsSchema.parse({
    version: versionRaw === undefined ? undefined : z.string().parse(versionRaw),
  });
}

export function inferInstallDir(currentVersion: string, execPath: string): string | null {
  if (currentVersion === "dev") {
    return null;
  }
  const executable = basename(execPath).toLowerCase();
  if (executable === "bun" || executable === "bunx") {
    return null;
  }
  return dirname(execPath);
}

export function createInstallerEnv(input: {
  env: NodeJS.ProcessEnv;
  currentVersion: string;
  execPath: string;
  requestedVersion?: string | undefined;
}): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = { ...input.env };
  if (input.requestedVersion) {
    env.LORE_VERSION = input.requestedVersion;
  }
  if (!env.LORE_INSTALL_DIR) {
    const inferredInstallDir = inferInstallDir(input.currentVersion, input.execPath);
    if (inferredInstallDir) {
      env.LORE_INSTALL_DIR = inferredInstallDir;
    }
  }
  return env;
}

export async function fetchInstallScript(url: string): Promise<string> {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to download installer from ${url} (HTTP ${response.status})`);
  }
  return response.text();
}

export async function runInstallerScript(script: string, env: NodeJS.ProcessEnv): Promise<void> {
  const scriptPath = join(
    tmpdir(),
    `lore-install-${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2)}.sh`,
  );
  await Bun.write(scriptPath, script);
  chmodSync(scriptPath, 0o700);
  try {
    const installer = Bun.spawn(["bash", scriptPath], {
      env,
      stdin: "inherit",
      stdout: "inherit",
      stderr: "inherit",
    });
    const exitCode = await installer.exited;
    if (exitCode !== 0) {
      throw new Error(`Installer failed with exit code ${exitCode}`);
    }
  } finally {
    rmSync(scriptPath, { force: true });
  }
}

export async function runUpdate(
  args: z.infer<typeof UpdateArgsSchema>,
  runtime: UpdateRuntime = {},
): Promise<void> {
  const installUrl = runtime.installUrl ?? process.env.LORE_INSTALL_URL ?? DEFAULT_INSTALL_URL;
  const fetcher = runtime.fetchInstallScript ?? fetchInstallScript;
  const installer = runtime.runInstallerScript ?? runInstallerScript;
  const env = createInstallerEnv({
    env: runtime.env ?? process.env,
    currentVersion: runtime.currentVersion ?? VERSION,
    execPath: runtime.execPath ?? process.execPath,
    requestedVersion: args.version,
  });
  if (args.version) {
    console.log(`Updating lore to ${args.version}...`);
  } else {
    console.log("Updating lore to latest...");
  }
  const script = await fetcher(installUrl);
  await installer(script, env);
  console.log("Update complete.");
}
