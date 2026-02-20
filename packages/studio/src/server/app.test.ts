import { describe, expect, test } from "bun:test";
import { createStudioApp, isAssetRequestPath } from "./app";

describe("studio app asset routing", () => {
  test("isAssetRequestPath matches vite asset URLs", () => {
    expect(isAssetRequestPath("/assets/index-abc123.js")).toBe(true);
    expect(isAssetRequestPath("/assets/styles-abc123.css")).toBe(true);
    expect(isAssetRequestPath("/tables/expenses")).toBe(false);
    expect(isAssetRequestPath("/schema")).toBe(false);
  });

  test("missing asset path returns 404 instead of index fallback", async () => {
    const app = createStudioApp();
    const response = await app.request("/assets/__missing_studio_asset__.js");

    expect(response.status).toBe(404);
    expect(await response.text()).toContain("Asset not found");
  });
});
