import { parsePositiveInt } from "./parse";

export type TimelineKind = "all" | "apply" | "revert";

export interface TimelineRouteSearch {
  page: number;
  kind: TimelineKind;
  table?: string | undefined;
}

export function validateTimelineSearch(raw: Record<string, unknown>): TimelineRouteSearch {
  const rawKind = raw.kind;
  const kind: TimelineKind = rawKind === "apply" || rawKind === "revert" ? rawKind : "all";
  const table = typeof raw.table === "string" && raw.table.trim().length > 0 ? raw.table.trim() : undefined;
  return {
    page: parsePositiveInt(raw.page, 1),
    kind,
    table,
  };
}
