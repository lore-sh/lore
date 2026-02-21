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

function requireOptionValue(args: string[], index: number, option: string): string {
  const value = args[index + 1];
  if (!value || value.startsWith("--")) {
    throw new Error(`remote connect requires a value for ${option}`);
  }
  return value;
}

function requireInlineOptionValue(value: string, option: string): string {
  if (value.length === 0) {
    throw new Error(`remote connect requires a value for ${option}`);
  }
  return value;
}

function parsePlatformValue(value: string): RemotePlatform {
  if (!isPlatform(value)) {
    throw new Error(`remote connect does not accept --platform=${value}. Use turso or libsql.`);
  }
  return value;
}

export function parseRemoteConnectArgs(args: string[]): ParsedRemoteConnectArgs {
  let platform: RemotePlatform | undefined;
  let url: string | undefined;
  let token: string | undefined;
  let clearToken = false;

  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i]!;
    if (arg === "--platform") {
      platform = parsePlatformValue(requireOptionValue(args, i, "--platform"));
      i += 1;
      continue;
    }
    if (arg.startsWith("--platform=")) {
      platform = parsePlatformValue(requireInlineOptionValue(arg.slice("--platform=".length), "--platform"));
      continue;
    }
    if (arg === "--url") {
      url = requireOptionValue(args, i, "--url");
      i += 1;
      continue;
    }
    if (arg.startsWith("--url=")) {
      url = requireInlineOptionValue(arg.slice("--url=".length), "--url");
      continue;
    }
    if (arg === "--token") {
      token = requireOptionValue(args, i, "--token");
      i += 1;
      continue;
    }
    if (arg.startsWith("--token=")) {
      token = requireInlineOptionValue(arg.slice("--token=".length), "--token");
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
