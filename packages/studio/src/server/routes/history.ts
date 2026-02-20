import { getStudioCommitDetail, listStudioHistory } from "@toss/core";
import { Hono } from "hono";
import { validator } from "hono/validator";
import type { StudioServerOptions } from "../types";
import { parsePositiveInt, singleValue } from "./query";

export function createHistoryRoutes(options: StudioServerOptions) {
  const historyQuery = validator("query", (query) => ({
    limit: parsePositiveInt(singleValue(query.limit)),
  }));

  return new Hono()
    .get("/api/history", historyQuery, (c) => {
      const query = c.req.valid("query");
      return c.json(
        listStudioHistory({
          ...options,
          limit: query.limit,
        }),
      );
    })
    .get("/api/history/:id", (c) => {
      return c.json(getStudioCommitDetail(c.req.param("id"), options));
    });
}
