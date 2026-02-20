import { Link, useNavigate, useParams, useSearch } from "@tanstack/react-router";
import { useSuspenseQuery } from "@tanstack/react-query";
import { flexRender, getCoreRowModel, useReactTable, type ColumnDef } from "@tanstack/react-table";
import { useMemo } from "react";
import { tableDataQueryOptions } from "../lib/queries";
import { validateTableSearch, type TableRouteSearch } from "../lib/table-search";

function formatCell(value: unknown): string {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return JSON.stringify(value);
}

export function TablePage() {
  const { name } = useParams({ from: "/tables/$name" });
  const search = useSearch({ from: "/tables/$name" });
  const navigate = useNavigate({ from: "/tables/$name" });
  const { data: tableData } = useSuspenseQuery(tableDataQueryOptions(name, search));

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

  function toggleSort(column: string): void {
    patchSearch((next) => {
      if (next.sortBy !== column) {
        next.sortBy = column;
        next.sortDir = "asc";
      } else {
        next.sortDir = next.sortDir === "asc" ? "desc" : "asc";
      }
      next.page = 1;
    });
  }

  function setFilter(column: string, value: string): void {
    patchSearch((next) => {
      const filters = { ...next.filters };
      const normalized = value.trim();
      if (normalized.length === 0) {
        delete filters[column];
      } else {
        filters[column] = normalized;
      }
      next.filters = filters;
      next.page = 1;
    });
  }

  function setPage(page: number): void {
    patchSearch((next) => {
      next.page = page < 1 ? 1 : page;
    });
  }

  function setPageSize(pageSize: number): void {
    patchSearch((next) => {
      next.pageSize = pageSize;
      next.page = 1;
    });
  }

  const columns = useMemo<ColumnDef<Record<string, unknown>>[]>(() => {
    return tableData.columns.map((column) => ({
      id: column.name,
      accessorKey: column.name,
      header: () => (
        <div className="space-y-2">
          <button
            type="button"
            onClick={() => {
              toggleSort(column.name);
            }}
            className="w-full rounded-lg border border-slate-200 bg-white px-2 py-1 text-left hover:border-slate-300"
          >
            <p className="font-semibold text-slate-900">{column.name}</p>
            <p className="text-xs text-slate-500">{column.type || "ANY"}</p>
            {tableData.sortBy === column.name ? (
              <p className="mt-1 text-[10px] uppercase tracking-[0.08em] text-amber-700">{tableData.sortDir}</p>
            ) : null}
          </button>
          <input
            value={search.filters[column.name] ?? ""}
            onChange={(event) => {
              setFilter(column.name, event.target.value);
            }}
            placeholder="filter..."
            className="w-full rounded-lg border border-slate-200 px-2 py-1 text-xs focus:border-slate-400 focus:outline-none"
          />
        </div>
      ),
      cell: (context) => <span className="font-mono text-xs text-slate-700">{formatCell(context.getValue())}</span>,
    }));
  }, [search.filters, tableData.columns, tableData.sortBy, tableData.sortDir]);

  const rows: Record<string, unknown>[] = tableData.rows;

  const table = useReactTable<Record<string, unknown>>({
    data: rows,
    columns,
    getCoreRowModel: getCoreRowModel(),
  });

  return (
    <section className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-xs uppercase tracking-[0.08em] text-slate-500">Table</p>
          <h1 className="text-2xl font-semibold text-slate-900">{name}</h1>
        </div>
        <Link to="/" className="rounded-full bg-slate-900 px-4 py-2 text-sm font-medium text-white hover:bg-slate-800">
          Back
        </Link>
      </div>

      <div className="overflow-auto rounded-2xl border border-slate-200 bg-white shadow-sm">
        <table className="min-w-full text-sm">
          <thead className="align-top">
            {table.getHeaderGroups().map((headerGroup) => (
              <tr key={headerGroup.id} className="border-b border-slate-100 bg-slate-50/70">
                {headerGroup.headers.map((header) => (
                  <th key={header.id} className="min-w-40 px-3 py-3 text-left text-xs font-medium text-slate-700">
                    {header.isPlaceholder ? null : flexRender(header.column.columnDef.header, header.getContext())}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.length === 0 ? (
              <tr>
                <td className="px-4 py-6 text-sm text-slate-500" colSpan={Math.max(columns.length, 1)}>
                  No rows matched this query.
                </td>
              </tr>
            ) : (
              table.getRowModel().rows.map((row) => (
                <tr key={row.id} className="border-t border-slate-100">
                  {row.getVisibleCells().map((cell) => (
                    <td key={cell.id} className="px-3 py-2 align-top">
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </td>
                  ))}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      <footer className="flex flex-wrap items-center justify-between gap-3 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
        <p className="text-sm text-slate-600">
          {tableData.totalRows} rows · page {tableData.page} / {tableData.totalPages}
        </p>
        <div className="flex items-center gap-2">
          <select
            value={tableData.pageSize}
            onChange={(event) => {
              setPageSize(Number.parseInt(event.target.value, 10));
            }}
            className="rounded-lg border border-slate-200 px-2 py-1 text-sm"
          >
            <option value={25}>25</option>
            <option value={50}>50</option>
            <option value={100}>100</option>
          </select>
          <button
            type="button"
            disabled={tableData.page <= 1}
            onClick={() => {
              setPage(tableData.page - 1);
            }}
            className="rounded-lg border border-slate-200 px-3 py-1 text-sm disabled:cursor-not-allowed disabled:opacity-50"
          >
            Prev
          </button>
          <button
            type="button"
            disabled={tableData.page >= tableData.totalPages}
            onClick={() => {
              setPage(tableData.page + 1);
            }}
            className="rounded-lg border border-slate-200 px-3 py-1 text-sm disabled:cursor-not-allowed disabled:opacity-50"
          >
            Next
          </button>
        </div>
      </footer>
    </section>
  );
}
