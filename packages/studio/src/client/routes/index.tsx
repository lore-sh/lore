import { Link } from "@tanstack/react-router";
import { useSuspenseQuery } from "@tanstack/react-query";
import { statusQueryOptions, tablesQueryOptions } from "../lib/queries";

function StatCard(props: { label: string; value: string }) {
  return (
    <article className="rounded-2xl border border-slate-200 bg-white/90 p-4 shadow-sm">
      <p className="text-xs uppercase tracking-[0.12em] text-slate-500">{props.label}</p>
      <p className="mt-2 text-2xl font-semibold text-slate-900">{props.value}</p>
    </article>
  );
}

export function DashboardPage() {
  const { data: tables } = useSuspenseQuery(tablesQueryOptions());
  const { data: status } = useSuspenseQuery(statusQueryOptions());
  const tableRows = tables.tables;
  const head = status.headCommit;
  return (
    <section className="space-y-6">
      <div className="grid gap-4 md:grid-cols-4">
        <StatCard label="User tables" value={String(status.tableCount)} />
        <StatCard label="Snapshots" value={String(status.snapshotCount)} />
        <StatCard label="Head kind" value={head?.kind ?? "none"} />
        <StatCard label="Last verified" value={status.lastVerifiedOkAt ?? "never"} />
      </div>

      <section className="overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">
        <div className="border-b border-slate-100 px-5 py-4">
          <h2 className="text-lg font-semibold">Tables</h2>
          <p className="text-sm text-slate-500">Click a table to inspect rows and query with URL state.</p>
        </div>
        {tableRows.length === 0 ? (
          <p className="px-5 py-6 text-sm text-slate-500">No user tables yet.</p>
        ) : (
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-left text-xs uppercase tracking-[0.08em] text-slate-500">
              <tr>
                <th className="px-5 py-3">Name</th>
                <th className="px-5 py-3">Rows</th>
                <th className="px-5 py-3">Columns</th>
                <th className="px-5 py-3">Last updated</th>
              </tr>
            </thead>
            <tbody>
              {tableRows.map((table) => (
                <tr key={table.name} className="border-t border-slate-100">
                  <td className="px-5 py-3">
                    <Link
                      to="/tables/$name"
                      params={{ name: table.name }}
                      search={{ page: 1, pageSize: 50, sortDir: "asc", filters: {} }}
                      className="font-medium text-slate-900 underline-offset-2 hover:underline"
                    >
                      {table.name}
                    </Link>
                  </td>
                  <td className="px-5 py-3 text-slate-700">{table.rowCount}</td>
                  <td className="px-5 py-3 text-slate-700">{table.columnCount}</td>
                  <td className="px-5 py-3 text-slate-500">{table.lastUpdatedAt ?? "n/a"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </section>
  );
}
