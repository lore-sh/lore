import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { createStudioApp } from "../../../src/server/app";

describe("studio route validation", () => {
  test("POST /api/tables/:name/rows/query returns 400 for invalid payload", async () => {
    const db = new Database(":memory:", { strict: true });
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
    db.close(false);

    expect(response.status).toBe(400);
    const payload = await response.json();
    expect(payload.code).toBe("VALIDATION_ERROR");
    expect(Array.isArray(payload.details)).toBe(true);
    expect(payload.details.length).toBeGreaterThan(0);
  });

  test("POST /api/tables/:name/rows/query returns VALIDATION_ERROR for malformed JSON", async () => {
    const db = new Database(":memory:", { strict: true });
    const app = createStudioApp(db);
    const response = await app.request("/api/tables/expenses/rows/query", {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: "{",
    });
    db.close(false);

    expect(response.status).toBe(400);
    const payload = await response.json();
    expect(payload.code).toBe("VALIDATION_ERROR");
    expect(Array.isArray(payload.details)).toBe(true);
    expect(payload.details[0]?.message).toContain("Malformed JSON");
  });

  test("GET /api/commits rejects invalid kind query", async () => {
    const db = new Database(":memory:", { strict: true });
    const app = createStudioApp(db);
    const response = await app.request("/api/commits?kind=invalid");
    db.close(false);

    expect(response.status).toBe(400);
    const payload = await response.json();
    expect(payload.code).toBe("VALIDATION_ERROR");
  });

  test("GET /api/commits/:id rejects empty param", async () => {
    const db = new Database(":memory:", { strict: true });
    const app = createStudioApp(db);
    const response = await app.request("/api/commits/%20");
    db.close(false);

    expect(response.status).toBe(400);
    const payload = await response.json();
    expect(payload.code).toBe("VALIDATION_ERROR");
  });

  test("GET /api/unknown returns JSON 404", async () => {
    const db = new Database(":memory:", { strict: true });
    const app = createStudioApp(db);
    const response = await app.request("/api/unknown");
    db.close(false);

    expect(response.status).toBe(404);
    const payload = await response.json();
    expect(payload).toEqual({
      code: "NOT_FOUND",
      message: "API route not found",
    });
  });
});
