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
import { createTestContext, withTestHome, withTmpDirCleanup } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

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
