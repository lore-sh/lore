import { chmodSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { CodedError } from "./error";
import { resolveHomeDir } from "./db";
import type { RemotePlatform } from "./sync";

export interface RemoteConfig {
  platform: RemotePlatform;
  url: string;
}

interface ConfigFile {
  remote?: {
    platform?: unknown;
    url?: unknown;
  };
}

interface CredentialEntry {
  token?: unknown;
}

interface CredentialsFile {
  turso?: CredentialEntry;
  libsql?: CredentialEntry;
}

function resolveTossDirPath(): string {
  return resolve(resolveHomeDir(), ".toss");
}

export function resolveConfigPath(): string {
  return resolve(resolveTossDirPath(), "config.json");
}

export function resolveCredentialsPath(): string {
  return resolve(resolveTossDirPath(), "credentials.json");
}

function ensureParentDirectory(path: string): void {
  mkdirSync(dirname(path), { recursive: true });
}

function readJsonFile(path: string): unknown | null {
  if (!existsSync(path)) {
    return null;
  }
  const text = readFileSync(path, "utf8");
  if (text.trim().length === 0) {
    throw new CodedError("CONFIG", `Config file is empty: ${path}`);
  }
  try {
    return JSON.parse(text) as unknown;
  } catch {
    throw new CodedError("CONFIG", `Config file is not valid JSON: ${path}`);
  }
}

function writeJsonFile(path: string, value: unknown): void {
  ensureParentDirectory(path);
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseNonEmptyString(value: unknown, fieldPath: string): string {
  if (typeof value !== "string") {
    throw new CodedError("CONFIG", `${fieldPath} must be a string`);
  }
  const normalized = value.trim();
  if (normalized.length === 0) {
    throw new CodedError("CONFIG", `${fieldPath} must not be empty`);
  }
  return normalized;
}

export function parseRemotePlatform(value: unknown, fieldPath = "platform"): RemotePlatform {
  if (value === "turso" || value === "libsql") {
    return value;
  }
  throw new CodedError("CONFIG", `${fieldPath} must be one of: turso, libsql`);
}

function parseRemoteConfigFromUnknown(value: unknown): RemoteConfig | null {
  if (!isRecord(value)) {
    throw new CodedError("CONFIG", "config.json root must be an object");
  }
  const parsed = value as ConfigFile;
  if (!parsed.remote) {
    return null;
  }
  const remote = parsed.remote;
  return {
    platform: parseRemotePlatform(remote.platform, "remote.platform"),
    url: parseNonEmptyString(remote.url, "remote.url"),
  };
}

function parseToken(value: unknown, fieldPath: string): string | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  return parseNonEmptyString(value, fieldPath);
}

function parseCredentialsFromUnknown(value: unknown): CredentialsFile {
  if (!isRecord(value)) {
    throw new CodedError("CONFIG", "credentials.json root must be an object");
  }
  const parsed = value as CredentialsFile;
  if (parsed.turso !== undefined && !isRecord(parsed.turso)) {
    throw new CodedError("CONFIG", "credentials.turso must be an object");
  }
  if (parsed.libsql !== undefined && !isRecord(parsed.libsql)) {
    throw new CodedError("CONFIG", "credentials.libsql must be an object");
  }
  return parsed;
}

function chmodCredentials(path: string): void {
  try {
    chmodSync(path, 0o600);
  } catch (error) {
    if (typeof error === "object" && error !== null && "code" in error) {
      const code = error.code;
      if (code === "ENOSYS" || code === "EINVAL" || code === "EPERM") {
        return;
      }
    }
    throw error;
  }
}

export function readRemoteConfig(): RemoteConfig | null {
  const parsed = readJsonFile(resolveConfigPath());
  if (parsed === null) {
    return null;
  }
  return parseRemoteConfigFromUnknown(parsed);
}

export function writeRemoteConfig(remote: RemoteConfig): void {
  const platform = parseRemotePlatform(remote.platform, "remote.platform");
  writeJsonFile(resolveConfigPath(), {
    remote: {
      platform,
      url: remote.url,
    },
  });
}

function readCredentials(): CredentialsFile {
  const parsed = readJsonFile(resolveCredentialsPath());
  if (parsed === null) {
    return {};
  }
  return parseCredentialsFromUnknown(parsed);
}

function writeCredentials(credentials: CredentialsFile): void {
  const path = resolveCredentialsPath();
  writeJsonFile(path, credentials);
  chmodCredentials(path);
}

function tokenFromEntry(entry: CredentialEntry | undefined, fieldPath: string): string | undefined {
  if (!entry) {
    return undefined;
  }
  return parseToken(entry.token, fieldPath);
}

function normalizeEnvToken(token: string | undefined): string | undefined {
  if (token === undefined) {
    return undefined;
  }
  const normalized = token.trim();
  return normalized.length === 0 ? undefined : normalized;
}

export function readAuthToken(platform: RemotePlatform): string | undefined {
  const normalizedPlatform = parseRemotePlatform(platform);
  const credentials = readCredentials();
  if (normalizedPlatform === "turso") {
    return tokenFromEntry(credentials.turso, "credentials.turso.token") ?? normalizeEnvToken(Bun.env.TURSO_AUTH_TOKEN);
  }
  return tokenFromEntry(credentials.libsql, "credentials.libsql.token");
}

export function writeAuthToken(platform: RemotePlatform, token: string): void {
  const normalizedPlatform = parseRemotePlatform(platform);
  const credentials = readCredentials();
  const normalized = parseNonEmptyString(token, `${normalizedPlatform} token`);
  if (normalizedPlatform === "turso") {
    credentials.turso = { token: normalized };
  } else {
    credentials.libsql = { token: normalized };
  }
  writeCredentials(credentials);
}

export function clearAuthToken(platform: RemotePlatform): void {
  const normalizedPlatform = parseRemotePlatform(platform);
  const credentials = readCredentials();
  if (normalizedPlatform === "turso") {
    delete credentials.turso;
  } else {
    delete credentials.libsql;
  }
  writeCredentials(credentials);
}
