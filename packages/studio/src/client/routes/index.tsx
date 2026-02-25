import { Link } from "@tanstack/react-router";
import { useSuspenseQuery } from "@tanstack/react-query";
import type { history, tableOverview } from "@lore/core";
import { CommitEntry } from "../components/commit-entry";
import { TableRow } from "../components/table-row";
import { formatBytes, formatRelativeTime } from "../lib/time";
import { historyQueryOptions, statusQueryOptions, tablesQueryOptions } from "../lib/queries";

function dbLabel(path: string): string {
  const normalized = path.replaceAll("\\", "/");
  const parts = normalized.split("/").filter((part) => part.length > 0);
  return parts[parts.length - 1] ?? path;
}

function totalRows(rowCounts: number[]): number {
  return rowCounts.reduce((sum, count) => sum + count, 0);
}

type CommitSummary = ReturnType<typeof history>[number];
type TableOverview = ReturnType<typeof tableOverview>[number];

function ActivityIcon() {
  return (
    <div className="ui-section-icon">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
        <polyline points="22 12 18 12 15 21 9 3 6 12 2 12" />
      </svg>
    </div>
  );
}

function TablesIcon() {
  return (
    <div className="ui-section-icon">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
        <ellipse cx="12" cy="5" rx="9" ry="3" />
        <path d="M21 12c0 1.66-4.03 3-9 3s-9-1.34-9-3" />
        <path d="M3 5v14c0 1.66 4.03 3 9 3s9-1.34 9-3V5" />
      </svg>
    </div>
  );
}

function ActivitySection({ history }: { history: CommitSummary[] }) {
  return (
    <section className="ui-surface">
      <header className="ui-section-head flex items-center justify-between">
        <div className="flex items-center gap-2">
          <ActivityIcon />
          <h2 className="ui-title">Activity</h2>
        </div>
        <Link to="/timeline" search={{ page: 1, kind: "all" }} className="ui-link">
          View all →
        </Link>
      </header>
      {history.length === 0 ? (
        <p className="ui-empty">No commits yet.</p>
      ) : (
        <ul className="ui-timeline">
          {history.map((commit) => (
            <CommitEntry key={commit.commitId} commit={commit} enableRevert />
          ))}
        </ul>
      )}
    </section>
  );
}

function TablesSection({ tables }: { tables: TableOverview[] }) {
  return (
    <section id="tables" className="ui-surface">
      <header className="ui-section-head">
        <div className="flex items-center gap-2">
          <TablesIcon />
          <h2 className="ui-title">Tables</h2>
        </div>
      </header>
      {tables.length === 0 ? (
        <p className="ui-empty">No tables yet. Data will appear here as you use Lore.</p>
      ) : (
        <div className="ui-table-list">
          {tables.map((table) => (
            <TableRow key={table.name} table={table} />
          ))}
        </div>
      )}
    </section>
  );
}

export function DashboardPage() {
  const { data: tables } = useSuspenseQuery(tablesQueryOptions());
  const { data: status } = useSuspenseQuery(statusQueryOptions());
  const { data: history } = useSuspenseQuery(historyQueryOptions({ limit: 20, page: 1 }));
  const tableRows = tables.tables;
  const rowCount = totalRows(tableRows.map((table) => table.rowCount));

  return (
    <section className="ui-stack-4">
      <div className="ui-stats">
        <div className="ui-stat">
          <span className="font-mono">{dbLabel(status.dbPath)}</span>
        </div>
        <div className="ui-stat">
          <strong>{tableRows.length}</strong> tables
        </div>
        <div className="ui-stat">
          <strong>{rowCount}</strong> rows
        </div>
        <div className="ui-stat">
          <strong>{status.storage.commitCount}</strong> commits
        </div>
      </div>

      <ActivitySection history={history} />
      <TablesSection tables={tableRows} />

      <p className="ui-health">
        {status.lastVerifiedOk ? "✓" : "!"} Verified {status.lastVerifiedAt ? formatRelativeTime(Date.parse(status.lastVerifiedAt)) : "never"}
        {" · "}
        {formatBytes(status.storage.estimatedHistoryBytes)} history
        {" · "}
        Sync: {status.sync.state} ({status.sync.pendingCommits} pending)
      </p>
    </section>
  );
}
