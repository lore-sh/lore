import { describe, expect, test } from "bun:test";
import {
  nextSortState,
  tableFilters,
  updateTableFilter,
  validateTableSearch,
  type TableRouteSearch,
} from "../../src/client/lib/table-search";
import { validateTimelineSearch } from "../../src/client/lib/timeline-search";

describe("search parsing", () => {
  test("parses table search with filters.* keys", () => {
    const parsed = validateTableSearch({
      tab: "data",
      page: "2",
      pageSize: "25",
      sortBy: "amount",
      sortDir: "desc",
      "filters.amount": "1200",
      "filters.category": "food",
      "filters.empty": "  ",
    });

    expect(parsed.page).toBe(2);
    expect(parsed.pageSize).toBe(25);
    expect(parsed.sortBy).toBe("amount");
    expect(parsed.sortDir).toBe("desc");
    expect(tableFilters(parsed)).toEqual({ amount: "1200", category: "food" });
  });

  test("updateTableFilter adds/removes filters and resets page", () => {
    const start = validateTableSearch({ page: 3, pageSize: 50 });
    const withFilter = updateTableFilter(start, "amount", "1200");
    expect(withFilter.page).toBe(1);
    expect(tableFilters(withFilter)).toEqual({ amount: "1200" });

    const cleared = updateTableFilter(withFilter, "amount", "");
    expect(tableFilters(cleared)).toEqual({});
  });

  test("sort state cycles asc -> desc -> none", () => {
    const base: TableRouteSearch = validateTableSearch({});
    const step1 = nextSortState(base, "amount");
    expect(step1).toEqual({ sortBy: "amount", sortDir: "asc" });

    const step2 = nextSortState({ ...base, ...step1 }, "amount");
    expect(step2).toEqual({ sortBy: "amount", sortDir: "desc" });

    const step3 = nextSortState({ ...base, ...step2 }, "amount");
    expect(step3).toEqual({ sortBy: undefined, sortDir: undefined });
  });

  test("parses timeline search", () => {
    expect(validateTimelineSearch({ page: "3", kind: "apply", table: "expenses" })).toEqual({
      page: 3,
      kind: "apply",
      table: "expenses",
    });

    expect(validateTimelineSearch({ page: "0", kind: "invalid" })).toEqual({
      page: 1,
      kind: "all",
      table: undefined,
    });
  });
});
