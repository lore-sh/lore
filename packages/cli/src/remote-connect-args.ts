import type { RemotePlatform } from "@toss/core";

export type ParsedRemoteConnectArgs = {
  interactive: true;
} | {
  interactive: false;
  platform: RemotePlatform;
  url: string;
  authToken?: string | null | undefined;
};

function isPlatform(value: string): value is RemotePlatform {
  return value === "turso" || value === "libsql";
}

function parsePlatformValue(value: string): RemotePlatform {
  if (!isPlatform(value)) {
    throw new Error(`remote connect does not accept --platform=${value}. Use turso or libsql.`);
  }
  return value;
}

function consumeOption(
  args: string[],
  index: number,
  arg: string,
  name: string,
): { value: string; skip: number } | null {
  if (arg === name) {
    const value = args[index + 1];
    if (!value || value.startsWith("--")) {
      throw new Error(`remote connect requires a value for ${name}`);
    }
    return { value, skip: 1 };
  }
  const prefix = `${name}=`;
  if (arg.startsWith(prefix)) {
    const value = arg.slice(prefix.length);
    if (value.length === 0) {
      throw new Error(`remote connect requires a value for ${name}`);
    }
    return { value, skip: 0 };
  }
  return null;
}

export function parseRemoteConnectArgs(args: string[]): ParsedRemoteConnectArgs {
  let platform: RemotePlatform | undefined;
  let url: string | undefined;
  let token: string | undefined;
  let clearToken = false;

  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i]!;
    const platformOpt = consumeOption(args, i, arg, "--platform");
    if (platformOpt) {
      platform = parsePlatformValue(platformOpt.value);
      i += platformOpt.skip;
      continue;
    }
    const urlOpt = consumeOption(args, i, arg, "--url");
    if (urlOpt) {
      url = urlOpt.value;
      i += urlOpt.skip;
      continue;
    }
    const tokenOpt = consumeOption(args, i, arg, "--token");
    if (tokenOpt) {
      token = tokenOpt.value;
      i += tokenOpt.skip;
      continue;
    }
    if (arg === "--clear-token") {
      clearToken = true;
      continue;
    }
    throw new Error(`remote connect does not accept argument: ${arg}`);
  }

  if (token !== undefined && clearToken) {
    throw new Error("remote connect does not allow --token with --clear-token.");
  }

  const hasNonInteractiveArg = platform !== undefined || url !== undefined || token !== undefined || clearToken;
  if (!hasNonInteractiveArg) {
    return { interactive: true };
  }
  if (!platform || !url) {
    throw new Error("remote connect non-interactive mode requires --platform <turso|libsql> and --url <url>.");
  }
  return { interactive: false, platform, url, authToken: clearToken ? null : token };
}
