import { describe, expect, test } from "bun:test";
import { resolveQueryView } from "../../src/client/lib/query-state";

describe("resolveQueryView", () => {
  test("returns loading while first fetch is pending", () => {
    const state = resolveQueryView({
      data: undefined,
      error: null,
      isPending: true,
      isLoadingError: false,
      isRefetchError: false,
    });

    expect(state).toEqual({
      kind: "loading",
    });
  });

  test("returns blocking error when first fetch fails", () => {
    const state = resolveQueryView({
      data: undefined,
      error: new Error("network down"),
      isPending: false,
      isLoadingError: true,
      isRefetchError: false,
    });

    expect(state).toEqual({
      kind: "error",
      message: "network down",
    });
  });

  test("returns data with background warning when refetch fails", () => {
    const state = resolveQueryView({
      data: { rows: 1 },
      error: new Error("refetch failed"),
      isPending: false,
      isLoadingError: false,
      isRefetchError: true,
    });

    expect(state).toEqual({
      kind: "data",
      data: { rows: 1 },
      refetchErrorMessage: "refetch failed",
    });
  });

  test("returns data without warning on normal success", () => {
    const state = resolveQueryView({
      data: { rows: 1 },
      error: null,
      isPending: false,
      isLoadingError: false,
      isRefetchError: false,
    });

    expect(state).toEqual({
      kind: "data",
      data: { rows: 1 },
      refetchErrorMessage: null,
    });
  });

  test("falls back to error when query reports error without data", () => {
    const state = resolveQueryView({
      data: undefined,
      error: new Error("refetch failed"),
      isPending: false,
      isLoadingError: false,
      isRefetchError: false,
    });

    expect(state).toEqual({
      kind: "error",
      message: "refetch failed",
    });
  });

  test("falls back to loading when query has no data and no error", () => {
    const state = resolveQueryView({
      data: undefined,
      error: null,
      isPending: false,
      isLoadingError: false,
      isRefetchError: false,
    });

    expect(state).toEqual({
      kind: "loading",
    });
  });
});
