import { describe, expect, test } from "bun:test";
import { toErrorFromPayload } from "../../src/client/lib/api";

describe("api error payload parsing", () => {
  test("parses legacy { code, message, details } payload", () => {
    const error = toErrorFromPayload(
      {
        code: "NOT_FOUND",
        message: "Table not found",
        details: [{ field: "table" }],
      },
      404,
    ) as Error & { code?: unknown; details?: unknown; status?: unknown };

    expect(error.message).toBe("Table not found");
    expect(error.code).toBe("NOT_FOUND");
    expect(error.status).toBe(404);
    expect(error.details).toEqual([{ field: "table" }]);
  });

  test("parses RFC7807 payload with toss code", () => {
    const error = toErrorFromPayload(
      {
        type: "https://docs.toss.sh/errors/not_found",
        title: "NOT_FOUND",
        status: 404,
        detail: "Table not found: missing",
        instance: "/api/tables/missing/history",
        code: "NOT_FOUND",
      },
      404,
    ) as Error & {
      code?: unknown;
      status?: unknown;
      type?: unknown;
      title?: unknown;
      instance?: unknown;
    };

    expect(error.message).toBe("Table not found: missing");
    expect(error.code).toBe("NOT_FOUND");
    expect(error.status).toBe(404);
    expect(error.type).toBe("https://docs.toss.sh/errors/not_found");
    expect(error.title).toBe("NOT_FOUND");
    expect(error.instance).toBe("/api/tables/missing/history");
  });

  test("falls back to generic error for unknown payloads", () => {
    const error = toErrorFromPayload({ ok: false }, 503) as Error & { status?: unknown };
    expect(error.message).toBe("Request failed with status 503");
    expect(error.status).toBe(503);
  });
});
