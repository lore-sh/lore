import { quoteIdentifier } from "./sql";

export function buildPkWhereClause(
  pk: Record<string, string>,
  fail: (message: string) => never,
): string {
  const keys = Object.keys(pk).sort((a, b) => a.localeCompare(b));
  if (keys.length === 0) {
    return fail("Primary key predicate must not be empty");
  }
  const clauses: string[] = [];
  for (const key of keys) {
    const literal = pk[key];
    if (!literal) {
      return fail(`Primary key literal is missing for column ${key}`);
    }
    const quoted = quoteIdentifier(key, { unsafe: true });
    clauses.push(literal.toUpperCase() === "NULL" ? `${quoted} IS NULL` : `${quoted} = ${literal}`);
  }
  return clauses.join(" AND ");
}

export function buildRowSelectSql(
  tableName: string,
  columns: string[],
  keyColumns: string[],
  whereClause: string | null,
): string {
  const quoteAliases = columns.map((_, i) => `__lore_quote_${i}`);
  const hexAliases = columns.map((_, i) => `__lore_hex_${i}`);
  const typeAliases = columns.map((_, i) => `__lore_type_${i}`);
  const parts: string[] = [];
  for (let i = 0; i < columns.length; i += 1) {
    const column = columns[i]!;
    const quotedColumn = quoteIdentifier(column, { unsafe: true });
    parts.push(`quote(${quotedColumn}) AS ${quoteIdentifier(quoteAliases[i]!, { unsafe: true })}`);
    parts.push(`hex(CAST(${quotedColumn} AS BLOB)) AS ${quoteIdentifier(hexAliases[i]!, { unsafe: true })}`);
    parts.push(`typeof(${quotedColumn}) AS ${quoteIdentifier(typeAliases[i]!, { unsafe: true })}`);
  }

  const orderBy = keyColumns.map((key) => `${quoteIdentifier(key, { unsafe: true })} ASC`).join(", ");
  const whereSql = whereClause ? ` WHERE ${whereClause}` : "";
  return `SELECT ${parts.join(", ")} FROM ${quoteIdentifier(tableName, { unsafe: true })}${whereSql} ORDER BY ${orderBy}`;
}
