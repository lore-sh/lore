import { Link, useNavigate, useParams, useSearch } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import { CommitEntry } from "../components/commit-entry";
import { DataGrid } from "../components/data-grid";
import { QueryBoundary } from "../components/query-boundary";
import {
  tableDataQueryOptions,
  tableHistoryQueryOptions,
  tableSchemaQueryOptions,
} from "../lib/queries";
import {
  tableFilters,
  nextSortState,
  validateTableSearch,
  type TableRouteSearch,
  type TableTab,
} from "../lib/table-search";

function tabClass(active: boolean): string {
  return active ? "ui-tab ui-tab-active" : "ui-tab";
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
      const filterKey = `filters.${column}`;
      const normalized = value.trim();
      if (normalized.length === 0) {
        delete next[filterKey];
      } else {
        next[filterKey] = normalized;
      }
      next.page = 1;
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
  const rowCount = dataQuery.data?.totalRows ?? schemaQuery.data?.rowCount ?? 0;
  const columnCount = dataQuery.data?.columns.length ?? schemaQuery.data?.columns.filter((column) => !column.hidden).length ?? 0;

  return (
    <section className="ui-stack-4">
      <header className="ui-table-header">
        <Link to="/" className="ui-back-link" aria-label="Back to overview">←</Link>
        <h1 className="ui-page-title">{name}</h1>
        <p className="ui-muted">{rowCount} rows · {columnCount} cols</p>
      </header>

      <nav className="ui-tabs" aria-label="Table tabs">
        <button type="button" className={tabClass(currentTab === "data")} onClick={() => setTab("data")}>Data</button>
        <button type="button" className={tabClass(currentTab === "schema")} onClick={() => setTab("schema")}>Schema</button>
        <button type="button" className={tabClass(currentTab === "history")} onClick={() => setTab("history")}>History</button>
      </nav>

      {currentTab === "data" && (
        <QueryBoundary query={dataQuery} loadingLabel="Loading data..." staleErrorLabel="Failed to refresh data, showing cached result">
          {(tableData) => (
            <DataGrid
              data={tableData}
              search={search}
              onSort={toggleSort}
              onFilter={setFilter}
              onPage={setPage}
              onPageSize={setPageSize}
            />
          )}
        </QueryBoundary>
      )}

      {currentTab === "schema" && (
        <QueryBoundary query={schemaQuery} loadingLabel="Loading schema..." staleErrorLabel="Failed to refresh schema, showing cached result">
          {(schemaData) => (
            <section className="ui-surface">
              <table className="ui-grid">
                <thead>
                  <tr>
                    <th>Column</th>
                    <th>Type</th>
                    <th>Constraints</th>
                    <th>Default</th>
                  </tr>
                </thead>
                <tbody>
                  {schemaData.columns
                    .filter((column) => !column.hidden)
                    .map((column) => {
                      const constraints = [column.primaryKey ? "PK" : null, column.notNull ? "NOT NULL" : null, column.unique ? "UNIQUE" : null]
                        .filter((part): part is string => part !== null)
                        .join(", ");
                      return (
                        <tr key={column.name}>
                          <td>{column.name}</td>
                          <td>{column.type || "ANY"}</td>
                          <td>{constraints || "—"}</td>
                          <td>{column.defaultValue ?? "—"}</td>
                        </tr>
                      );
                    })}
                </tbody>
              </table>
            </section>
          )}
        </QueryBoundary>
      )}

      {currentTab === "history" && (
        <QueryBoundary query={historyQuery} loadingLabel="Loading history..." staleErrorLabel="Failed to refresh history, showing cached result">
          {(historyData) =>
          historyData.length > 0 ? (
            <section className="ui-surface">
              <ul className="ui-timeline">
                {historyData.map((commit) => (
                  <CommitEntry key={commit.commitId} commit={commit} showAffectedTables={false} enableRevert />
                ))}
              </ul>
            </section>
          ) : (
            <section className="ui-surface">
              <p className="ui-empty">No history for this table.</p>
            </section>
          )}
        </QueryBoundary>
      )}
    </section>
  );
}
