import { getStudioCommitDetail, listStudioHistory, type CommitKind } from "@toss/core";
import { Hono } from "hono";
import { validator } from "hono/validator";
import { parsePositiveInt, singleValue } from "./query";

export function createHistoryRoutes() {
  const historyQuery = validator("query", (query) => {
    const rawKind = singleValue(query.kind);
    const kind: CommitKind | undefined = rawKind === "apply" || rawKind === "revert" ? rawKind : undefined;
    const rawTable = singleValue(query.table);
    return {
      limit: parsePositiveInt(singleValue(query.limit)),
      page: parsePositiveInt(singleValue(query.page)),
      kind,
      table: typeof rawTable === "string" && rawTable.trim().length > 0 ? rawTable : undefined,
    };
  });

  return new Hono()
    .get("/api/history", historyQuery, (c) => {
      const query = c.req.valid("query");
      return c.json(
        listStudioHistory({
          limit: query.limit,
          page: query.page,
          kind: query.kind,
          table: query.table,
        }),
      );
    })
    .get("/api/history/:id", (c) => {
      return c.json(getStudioCommitDetail(c.req.param("id")));
    });
}
