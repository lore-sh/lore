export function toJson(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

export function printTable(rows: Array<Record<string, unknown>>): string {
  if (rows.length === 0) {
    return "(no rows)";
  }

  const headers = Object.keys(rows[0] ?? {});
  const widths = headers.map((header) => Math.max(header.length, ...rows.map((row) => String(row[header] ?? "null").length)));

  const makeLine = (values: string[]): string =>
    `| ${values.map((value, index) => value.padEnd(widths[index] ?? value.length)).join(" | ")} |`;

  const divider = `+-${widths.map((width) => "-".repeat(width)).join("-+-")}-+`;
  const headerLine = makeLine(headers);
  const lines = rows.map((row) => makeLine(headers.map((header) => String(row[header] ?? "null"))));

  return [divider, headerLine, divider, ...lines, divider].join("\n");
}
