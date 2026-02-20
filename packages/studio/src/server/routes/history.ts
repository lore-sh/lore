import { getStudioCommitDetail, listStudioHistory } from "@toss/core";
import { Hono } from "hono";
import { validator } from "hono/validator";
import { parsePositiveInt, singleValue } from "./query";

export function createHistoryRoutes() {
  const historyQuery = validator("query", (query) => ({
    limit: parsePositiveInt(singleValue(query.limit)),
  }));

  return new Hono()
    .get("/api/history", historyQuery, (c) => {
      const query = c.req.valid("query");
      return c.json(
        listStudioHistory({
          limit: query.limit,
        }),
      );
    })
    .get("/api/history/:id", (c) => {
      return c.json(getStudioCommitDetail(c.req.param("id")));
    });
}
