import { describe, expect, test } from "bun:test";
import { parseCloneArgs, parseClonePlatform } from "../../src/commands/clone";

describe("clone command", () => {
  test("parseClonePlatform rejects invalid platforms", () => {
    expect(() => parseClonePlatform("mysql")).toThrow("clone does not accept --platform=mysql");
  });

  test("parseCloneArgs parses url and platform", () => {
    const parsed = parseCloneArgs(["libsql://db.turso.io", "--platform", "turso"]);
    expect(parsed).toEqual({ platform: "turso", url: "libsql://db.turso.io", forceNew: false });
  });

  test("parseCloneArgs supports --force-new", () => {
    const parsed = parseCloneArgs(["libsql://db.turso.io", "--platform", "libsql", "--force-new"]);
    expect(parsed.forceNew).toBe(true);
  });

  test("parseCloneArgs requires url and platform", () => {
    expect(() => parseCloneArgs([])).toThrow();
    expect(() => parseCloneArgs(["libsql://db.turso.io"])).toThrow();
  });

  test("parseCloneArgs rejects unsupported URL schemes", () => {
    expect(() => parseCloneArgs(["file:/tmp/remote.db", "--platform", "libsql"])).toThrow(
      "Remote URL scheme is not supported",
    );
  });
});
