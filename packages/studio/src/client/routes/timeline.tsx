import { useSuspenseQuery } from "@tanstack/react-query";
import { useNavigate, useSearch } from "@tanstack/react-router";
import { CommitEntry } from "../components/commit-entry";
import { historyQueryOptions, tablesQueryOptions } from "../lib/queries";
import { validateTimelineSearch, type TimelineKind, type TimelineRouteSearch } from "../lib/timeline-search";

const PAGE_SIZE = 50;

function kindButton(active: boolean): string {
  return active ? "ui-segment ui-segment-active" : "ui-segment";
}

export function TimelinePage() {
  const search = useSearch({ from: "/timeline" });
  const navigate = useNavigate({ from: "/timeline" });
  const { data: tables } = useSuspenseQuery(tablesQueryOptions());
  const { data: history } = useSuspenseQuery(
    historyQueryOptions({
      limit: PAGE_SIZE,
      page: search.page,
      kind: search.kind === "all" ? undefined : search.kind,
      table: search.table,
    }),
  );

  function patchSearch(update: (next: TimelineRouteSearch) => void): void {
    void navigate({
      to: "/timeline",
      search: (prev) => {
        const next = validateTimelineSearch(prev);
        update(next);
        return next;
      },
    });
  }

  function setKind(kind: TimelineKind): void {
    patchSearch((next) => {
      next.kind = kind;
      next.page = 1;
    });
  }

  function setTable(value: string): void {
    patchSearch((next) => {
      next.table = value.length > 0 ? value : undefined;
      next.page = 1;
    });
  }

  function setPage(page: number): void {
    patchSearch((next) => {
      next.page = Math.max(1, page);
    });
  }

  return (
    <section className="ui-stack-4">
      <div className="ui-filter-bar">
        <div className="ui-segment-group">
          <button type="button" className={kindButton(search.kind === "all")} onClick={() => setKind("all")}>All</button>
          <button type="button" className={kindButton(search.kind === "apply")} onClick={() => setKind("apply")}>Apply</button>
          <button type="button" className={kindButton(search.kind === "revert")} onClick={() => setKind("revert")}>Revert</button>
        </div>
        <label className="ui-inline-field">
          <span className="ui-soft">Table</span>
          <select
            value={search.table ?? ""}
            className="ui-select"
            onChange={(event) => {
              setTable(event.target.value);
            }}
          >
            <option value="">All</option>
            {tables.tables.map((table) => (
              <option key={table.name} value={table.name}>{table.name}</option>
            ))}
          </select>
        </label>
      </div>

      <section className="ui-surface">
        {history.length === 0 ? (
          <p className="ui-empty">No commits found.</p>
        ) : (
          <ul className="ui-timeline">
            {history.map((commit) => (
              <CommitEntry key={commit.commitId} commit={commit} enableRevert />
            ))}
          </ul>
        )}
      </section>

      <footer className="ui-grid-footer">
        <p className="ui-muted">Page {search.page}</p>
        <div className="ui-grid-controls">
          <button type="button" className="ui-btn-ghost" disabled={search.page <= 1} onClick={() => setPage(search.page - 1)}>
            Prev
          </button>
          <button
            type="button"
            className="ui-btn-ghost"
            disabled={history.length < PAGE_SIZE}
            onClick={() => setPage(search.page + 1)}
          >
            Next
          </button>
        </div>
      </footer>
    </section>
  );
}
