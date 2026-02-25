import { describe, expect, test } from "bun:test";
import { initDb, openDb } from "@lore/core";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createStudioApp } from "../../../src/server/app";

describe("studio route validation", () => {
  test("POST /api/tables/:name/rows/query returns 400 for invalid payload", async () => {
    const dir = mkdtempSync(join(tmpdir(), "studio-validation-test-"));
    const dbPath = join(dir, "lore.db");
    await initDb({ dbPath });
    const db = openDb(dbPath);
    const app = createStudioApp(db);
    const response = await app.request("/api/tables/expenses/rows/query", {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({
        page: 0,
        sortDir: "sideways",
        filters: {
          amount: 1200,
        },
      }),
    });
    db.$client.close(false);
    rmSync(dir, { recursive: true, force: true });

    expect(response.status).toBe(400);
    const payload = await response.json();
    expect(payload.code).toBe("VALIDATION_ERROR");
    expect(Array.isArray(payload.details)).toBe(true);
    expect(payload.details.length).toBeGreaterThan(0);
  });

  test("POST /api/tables/:name/rows/query returns VALIDATION_ERROR for malformed JSON", async () => {
    const dir = mkdtempSync(join(tmpdir(), "studio-validation-test-"));
    const dbPath = join(dir, "lore.db");
    await initDb({ dbPath });
    const db = openDb(dbPath);
    const app = createStudioApp(db);
    const response = await app.request("/api/tables/expenses/rows/query", {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: "{",
    });
    db.$client.close(false);
    rmSync(dir, { recursive: true, force: true });

    expect(response.status).toBe(400);
    const payload = await response.json();
    expect(payload.code).toBe("VALIDATION_ERROR");
    expect(Array.isArray(payload.details)).toBe(true);
    expect(payload.details[0]?.message).toContain("Malformed JSON");
  });

  test("GET /api/commits rejects invalid kind query", async () => {
    const dir = mkdtempSync(join(tmpdir(), "studio-validation-test-"));
    const dbPath = join(dir, "lore.db");
    await initDb({ dbPath });
    const db = openDb(dbPath);
    const app = createStudioApp(db);
    const response = await app.request("/api/commits?kind=invalid");
    db.$client.close(false);
    rmSync(dir, { recursive: true, force: true });

    expect(response.status).toBe(400);
    const payload = await response.json();
    expect(payload.code).toBe("VALIDATION_ERROR");
  });

  test("GET /api/commits/:id rejects empty param", async () => {
    const dir = mkdtempSync(join(tmpdir(), "studio-validation-test-"));
    const dbPath = join(dir, "lore.db");
    await initDb({ dbPath });
    const db = openDb(dbPath);
    const app = createStudioApp(db);
    const response = await app.request("/api/commits/%20");
    db.$client.close(false);
    rmSync(dir, { recursive: true, force: true });

    expect(response.status).toBe(400);
    const payload = await response.json();
    expect(payload.code).toBe("VALIDATION_ERROR");
  });

  test("GET /api/unknown returns JSON 404", async () => {
    const dir = mkdtempSync(join(tmpdir(), "studio-validation-test-"));
    const dbPath = join(dir, "lore.db");
    await initDb({ dbPath });
    const db = openDb(dbPath);
    const app = createStudioApp(db);
    const response = await app.request("/api/unknown");
    db.$client.close(false);
    rmSync(dir, { recursive: true, force: true });

    expect(response.status).toBe(404);
    const payload = await response.json();
    expect(payload).toEqual({
      code: "NOT_FOUND",
      message: "API route not found",
    });
  });
});
