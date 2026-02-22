import { Link } from "@tanstack/react-router";
import { useSuspenseQuery } from "@tanstack/react-query";
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

export function DashboardPage() {
  const { data: tables } = useSuspenseQuery(tablesQueryOptions());
  const { data: status } = useSuspenseQuery(statusQueryOptions());
  const { data: history } = useSuspenseQuery(historyQueryOptions({ limit: 20, page: 1 }));
  const tableRows = tables.tables;
  const rowCount = totalRows(tableRows.map((table) => table.rowCount));
  const context = `${dbLabel(status.dbPath)} · ${tableRows.length} tables · ${rowCount} rows · ${status.storage.commitCount} commits`;
  const compactLayout = tableRows.length <= 5;

  return (
    <section className="ui-stack-4">
      <p className="ui-context">{context}</p>

      {compactLayout ? (
        <div className="ui-overview-grid">
          <section className="ui-surface">
            <header className="ui-section-head">
              <h2 className="ui-title">Activity</h2>
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
            <div className="ui-section-foot">
              <Link to="/timeline" search={{ page: 1, kind: "all" }} className="ui-link">
                View all in Timeline
              </Link>
            </div>
          </section>

          <section id="tables" className="ui-surface">
            <header className="ui-section-head">
              <h2 className="ui-title">Tables</h2>
            </header>
            {tableRows.length === 0 ? (
              <p className="ui-empty">No tables yet. Data will appear here as you use toss.</p>
            ) : (
              <ul className="ui-table-list">
                {tableRows.map((table) => (
                  <TableRow key={table.name} table={table} />
                ))}
              </ul>
            )}
          </section>
        </div>
      ) : (
        <>
          <section id="tables" className="ui-surface">
            <header className="ui-section-head">
              <h2 className="ui-title">Tables</h2>
            </header>
            {tableRows.length === 0 ? (
              <p className="ui-empty">No tables yet. Data will appear here as you use toss.</p>
            ) : (
              <ul className="ui-table-list">
                {tableRows.map((table) => (
                  <TableRow key={table.name} table={table} />
                ))}
              </ul>
            )}
          </section>

          <section className="ui-surface">
            <header className="ui-section-head">
              <h2 className="ui-title">Activity</h2>
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
            <div className="ui-section-foot">
              <Link to="/timeline" search={{ page: 1, kind: "all" }} className="ui-link">
                View all in Timeline
              </Link>
            </div>
          </section>
        </>
      )}

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
