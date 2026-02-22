import { Link, useNavigate, useParams, useSearch } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import { CommitEntry } from "../components/commit-entry";
import { DataGrid } from "../components/data-grid";
import {
  tableDataQueryOptions,
  tableHistoryQueryOptions,
  tableSchemaQueryOptions,
} from "../lib/queries";
import {
  tableFilters,
  nextSortState,
  updateTableFilter,
  validateTableSearch,
  type TableRouteSearch,
  type TableTab,
} from "../lib/table-search";

function tabClass(active: boolean): string {
  return active ? "ui-tab ui-tab-active" : "ui-tab";
}

function queryErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

export function TablePage() {
  const { name } = useParams({ from: "/tables/$name" });
  const search = useSearch({ from: "/tables/$name" });
  const navigate = useNavigate({ from: "/tables/$name" });

  function patchSearch(update: (next: TableRouteSearch) => void): void {
    void navigate({
      to: "/tables/$name",
      params: { name },
      search: (prev) => {
        const next = validateTableSearch(prev);
        update(next);
        return next;
      },
    });
  }

  function setTab(tab: TableTab): void {
    patchSearch((next) => {
      next.tab = tab;
    });
  }

  function setPage(page: number): void {
    patchSearch((next) => {
      next.page = Math.max(1, page);
    });
  }

  function setPageSize(pageSize: number): void {
    patchSearch((next) => {
      next.pageSize = pageSize;
      next.page = 1;
    });
  }

  function setFilter(column: string, value: string): void {
    patchSearch((next) => {
      const updated = updateTableFilter(next, column, value);
      for (const [key, entry] of Object.entries(updated)) {
        next[key] = entry;
      }
      for (const key of Object.keys(next)) {
        if (key.startsWith("filters.") && !(key in updated)) {
          delete next[key];
        }
      }
    });
  }

  function toggleSort(column: string): void {
    patchSearch((next) => {
      const sort = nextSortState(next, column);
      next.sortBy = sort.sortBy;
      next.sortDir = sort.sortDir;
      next.page = 1;
    });
  }

  const currentTab = search.tab;
  const filters = tableFilters(search);
  const dataQuery = useQuery({
    ...tableDataQueryOptions(name, {
      page: search.page,
      pageSize: search.pageSize,
      sortBy: search.sortBy,
      sortDir: search.sortDir,
      filters,
    }),
    enabled: currentTab === "data",
  });
  const schemaQuery = useQuery({
    ...tableSchemaQueryOptions(name),
    enabled: currentTab === "schema",
  });
  const historyQuery = useQuery({
    ...tableHistoryQueryOptions(name, 50),
    enabled: currentTab === "history",
  });
  const data = dataQuery.data ?? null;
  const schema = schemaQuery.data ?? null;
  const history = historyQuery.data ?? null;

  const rowCount = data?.totalRows ?? schema?.rowCount ?? 0;
  const columnCount = data?.columns.length ?? schema?.columns.filter((column) => !column.hidden).length ?? 0;

  return (
    <section className="ui-stack-4">
      <header className="ui-table-header">
        <Link to="/" className="ui-link">←</Link>
        <h1 className="ui-title">{name}</h1>
        <p className="ui-muted">{rowCount} rows · {columnCount} columns</p>
      </header>

      <nav className="ui-tabs" aria-label="Table tabs">
        <button type="button" className={tabClass(currentTab === "data")} onClick={() => setTab("data")}>Data</button>
        <button type="button" className={tabClass(currentTab === "schema")} onClick={() => setTab("schema")}>Schema</button>
        <button type="button" className={tabClass(currentTab === "history")} onClick={() => setTab("history")}>History</button>
      </nav>

      {currentTab === "data" && data ? (
        <DataGrid
          data={data}
          search={search}
          onSort={toggleSort}
          onFilter={setFilter}
          onPage={setPage}
          onPageSize={setPageSize}
        />
      ) : currentTab === "data" && dataQuery.isError ? (
        <p className="ui-error">{queryErrorMessage(dataQuery.error)}</p>
      ) : currentTab === "data" ? (
        <p className="ui-muted">Loading data...</p>
      ) : null}

      {currentTab === "schema" && schema ? (
        <section className="ui-surface">
          <table className="ui-grid">
            <thead>
              <tr>
                <th>COLUMN</th>
                <th>TYPE</th>
                <th>CONSTRAINTS</th>
                <th>DEFAULT</th>
              </tr>
            </thead>
            <tbody>
              {schema.columns
                .filter((column) => !column.hidden)
                .map((column) => {
                  const constraints = [column.primaryKey ? "PK" : null, column.notNull ? "NOT NULL" : null, column.unique ? "UNIQUE" : null]
                    .filter((part): part is string => part !== null)
                    .join(", ");
                  return (
                    <tr key={column.name}>
                      <td>{column.name}</td>
                      <td>{column.type || "ANY"}</td>
                      <td>{constraints || "-"}</td>
                      <td>{column.defaultValue ?? "-"}</td>
                    </tr>
                  );
                })}
            </tbody>
          </table>
        </section>
      ) : currentTab === "schema" && schemaQuery.isError ? (
        <p className="ui-error">{queryErrorMessage(schemaQuery.error)}</p>
      ) : currentTab === "schema" ? (
        <p className="ui-muted">Loading schema...</p>
      ) : null}

      {currentTab === "history" ? (
        <section className="ui-surface">
          {historyQuery.isPending ? (
            <p className="ui-muted">Loading history...</p>
          ) : historyQuery.isError ? (
            <p className="ui-error">{queryErrorMessage(historyQuery.error)}</p>
          ) : history && history.length > 0 ? (
            <ul className="ui-timeline">
              {history.map((commit) => (
                <CommitEntry key={commit.commitId} commit={commit} showAffectedTables={false} enableRevert />
              ))}
            </ul>
          ) : (
            <p className="ui-empty">No history for this table.</p>
          )}
        </section>
      ) : null}
    </section>
  );
}
