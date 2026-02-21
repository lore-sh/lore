import { Link } from "@tanstack/react-router";
import { useSuspenseQuery } from "@tanstack/react-query";
import { formatTimestamp } from "../lib/time";
import { statusQueryOptions, tablesQueryOptions } from "../lib/queries";

function StatCard(props: { label: string; value: string }) {
  return (
    <article className="ui-surface-soft">
      <p className="text-xs uppercase tracking-[0.12em] text-fg-soft">{props.label}</p>
      <p className="mt-2 text-2xl font-semibold text-fg">{props.value}</p>
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
        <StatCard label="Last verified" value={status.lastVerifiedAt ?? "never"} />
      </div>

      <section className="ui-surface">
        <div className="ui-section-head">
          <h2 className="text-lg font-semibold">Tables</h2>
          <p className="text-sm text-fg-soft">Click a table to inspect rows and query with URL state.</p>
        </div>
        {tableRows.length === 0 ? (
          <p className="px-5 py-6 text-sm text-fg-soft">No user tables yet.</p>
        ) : (
          <table className="min-w-full text-sm">
            <thead className="ui-table-head">
              <tr>
                <th className="px-5 py-3">Name</th>
                <th className="px-5 py-3">Rows</th>
                <th className="px-5 py-3">Columns</th>
                <th className="px-5 py-3">Last updated</th>
              </tr>
            </thead>
            <tbody>
              {tableRows.map((table) => (
                <tr key={table.name} className="ui-table-row">
                  <td className="px-5 py-3">
                    <Link
                      to="/tables/$name"
                      params={{ name: table.name }}
                      search={{ page: 1, pageSize: 50, sortDir: "asc", filters: {} }}
                      className="ui-link"
                    >
                      {table.name}
                    </Link>
                  </td>
                  <td className="px-5 py-3 text-fg-muted">{table.rowCount}</td>
                  <td className="px-5 py-3 text-fg-muted">{table.columnCount}</td>
                  <td className="px-5 py-3 text-fg-soft">{formatTimestamp(table.lastUpdatedAt)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </section>
  );
}
