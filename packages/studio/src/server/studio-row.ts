import type { JsonObject } from "@lore/core";

export type StudioCellValue = string | number | boolean | null;
export type StudioRow = Record<string, StudioCellValue>;

export function toStudioRow(row: JsonObject): StudioRow {
  const output: StudioRow = {};
  for (const [key, value] of Object.entries(row)) {
    if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      output[key] = value;
      continue;
    }
    output[key] = JSON.stringify(value);
  }
  return output;
}
