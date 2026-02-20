import { useSuspenseQuery } from "@tanstack/react-query";
import { schemaQueryOptions } from "../lib/queries";

export function SchemaPage() {
  const { data: schema } = useSuspenseQuery(schemaQueryOptions());
  const tables = schema.tables;
  if (tables.length === 0) {
    return <p className="text-sm text-slate-500">No tables available.</p>;
  }

  return (
    <section className="space-y-5">
      {tables.map((table) => (
        <article key={table.name} className="overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">
          <header className="border-b border-slate-100 px-5 py-4">
            <h2 className="text-lg font-semibold">{table.name}</h2>
            <p className="text-sm text-slate-500">{table.rowCount} rows</p>
          </header>
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-left text-xs uppercase tracking-[0.08em] text-slate-500">
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
                  <tr key={column.name} className="border-t border-slate-100">
                    <td className="px-5 py-3 font-medium text-slate-900">{column.name}</td>
                    <td className="px-5 py-3 text-slate-700">{column.type || "ANY"}</td>
                    <td className="px-5 py-3 text-slate-600">{constraints || "-"}</td>
                    <td className="px-5 py-3 font-mono text-xs text-slate-500">{column.defaultValue ?? "-"}</td>
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
