import { describe, expect, test } from "bun:test";
import { statSync } from "node:fs";
import {
  readAuthToken,
  readRemoteConfig,
  resolveConfigPath,
  resolveCredentialsPath,
  writeAuthToken,
  writeRemoteConfig,
} from "../src";
import { createTestContext, withTmpDirCleanup } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

interface EnvSnapshot {
  HOME?: string | undefined;
  USERPROFILE?: string | undefined;
  TURSO_AUTH_TOKEN?: string | undefined;
}

function captureEnv(): EnvSnapshot {
  return {
    HOME: process.env.HOME,
    USERPROFILE: process.env.USERPROFILE,
    TURSO_AUTH_TOKEN: process.env.TURSO_AUTH_TOKEN,
  };
}

function restoreEnv(snapshot: EnvSnapshot): void {
  if (snapshot.HOME === undefined) {
    delete process.env.HOME;
  } else {
    process.env.HOME = snapshot.HOME;
  }
  if (snapshot.USERPROFILE === undefined) {
    delete process.env.USERPROFILE;
  } else {
    process.env.USERPROFILE = snapshot.USERPROFILE;
  }
  if (snapshot.TURSO_AUTH_TOKEN === undefined) {
    delete process.env.TURSO_AUTH_TOKEN;
  } else {
    process.env.TURSO_AUTH_TOKEN = snapshot.TURSO_AUTH_TOKEN;
  }
}

function withTestHome<T>(home: string, run: () => T): T {
  const snapshot = captureEnv();
  process.env.HOME = home;
  process.env.USERPROFILE = home;
  delete process.env.TURSO_AUTH_TOKEN;
  try {
    return run();
  } finally {
    restoreEnv(snapshot);
  }
}

describe("config", () => {
  testWithTmp("read/write remote config roundtrip", () => {
    const { dir } = createTestContext();
    withTestHome(dir, () => {
      expect(readRemoteConfig()).toBeNull();
      writeRemoteConfig({
        platform: "turso",
        url: "libsql://mydb-xxx.turso.io",
        dbName: "mydb-xxx",
        autoSync: true,
      });
      expect(readRemoteConfig()).toEqual({
        platform: "turso",
        url: "libsql://mydb-xxx.turso.io",
        dbName: "mydb-xxx",
        autoSync: true,
      });
      expect(resolveConfigPath()).toBe(`${dir}/.toss/config.json`);
    });
  });

  testWithTmp("credentials token is preferred over TURSO_AUTH_TOKEN", () => {
    const { dir } = createTestContext();
    withTestHome(dir, () => {
      process.env.TURSO_AUTH_TOKEN = "token-from-env";
      writeAuthToken("turso", "token-from-file");
      expect(readAuthToken("turso")).toBe("token-from-file");
    });
  });

  testWithTmp("TURSO_AUTH_TOKEN is used when credentials file is missing", () => {
    const { dir } = createTestContext();
    withTestHome(dir, () => {
      process.env.TURSO_AUTH_TOKEN = "token-from-env";
      expect(readAuthToken("turso")).toBe("token-from-env");
    });
  });

  testWithTmp("credentials file is chmod 600", () => {
    const { dir } = createTestContext();
    withTestHome(dir, () => {
      writeAuthToken("turso", "token-from-file");
      const path = resolveCredentialsPath();
      if (process.platform !== "win32") {
        expect(statSync(path).mode & 0o777).toBe(0o600);
      }
    });
  });
});
