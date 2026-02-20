import { useSuspenseQuery } from "@tanstack/react-query";
import { schemaQueryOptions } from "../lib/queries";

export function SchemaPage() {
  const { data: schema } = useSuspenseQuery(schemaQueryOptions());
  const tables = schema.tables;
  if (tables.length === 0) {
    return <p className="text-sm text-fg-soft">No tables available.</p>;
  }

  return (
    <section className="space-y-5">
      {tables.map((table) => (
        <article key={table.name} className="ui-surface">
          <header className="ui-section-head">
            <h2 className="text-lg font-semibold">{table.name}</h2>
            <p className="text-sm text-fg-soft">{table.rowCount} rows</p>
          </header>
          <table className="min-w-full text-sm">
            <thead className="ui-table-head">
              <tr>
                <th className="px-5 py-3">Column</th>
                <th className="px-5 py-3">Type</th>
                <th className="px-5 py-3">Constraints</th>
                <th className="px-5 py-3">Default</th>
              </tr>
            </thead>
            <tbody>
              {table.columns.map((column) => {
                const constraints = [
                  column.primaryKey && "PK",
                  column.notNull && "NOT NULL",
                  column.unique && "UNIQUE",
                ]
                  .filter(Boolean)
                  .join(", ");
                return (
                  <tr key={column.name} className="ui-table-row">
                    <td className="px-5 py-3 font-medium text-fg">{column.name}</td>
                    <td className="px-5 py-3 text-fg-muted">{column.type || "ANY"}</td>
                    <td className="px-5 py-3 text-fg-muted">{constraints || "-"}</td>
                    <td className="px-5 py-3 font-mono text-xs text-fg-soft">{column.defaultValue ?? "-"}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </article>
      ))}
    </section>
  );
}
