import { flexRender, getCoreRowModel, useReactTable, type ColumnDef } from "@tanstack/react-table";
import { useMemo, type ReactNode } from "react";
import type { StudioCellValue } from "../../server/studio-row";
import type { TableData } from "../lib/api";
import { tableFilterValue, type TableRouteSearch } from "../lib/table-search";

interface DataGridProps {
  data: TableData;
  search: TableRouteSearch;
  onSort: (column: string) => void;
  onFilter: (column: string, value: string) => void;
  onPage: (page: number) => void;
  onPageSize: (size: number) => void;
}

function renderCell(value: StudioCellValue): ReactNode {
  if (value === null) {
    return <span className="ui-cell-null">NULL</span>;
  }
  if (typeof value === "number") {
    return <span className="ui-cell-number">{value}</span>;
  }
  if (typeof value === "boolean") {
    return <span>{String(value)}</span>;
  }
  if (value.length > 80) {
    return <span title={value}>{value.slice(0, 80)}...</span>;
  }
  return <span>{value}</span>;
}

export function DataGrid({ data, search, onSort, onFilter, onPage, onPageSize }: DataGridProps) {
  const columns = useMemo<ColumnDef<Record<string, StudioCellValue>>[]>(() => {
    return data.columns.map((column) => {
      let arrow = "";
      if (search.sortBy === column.name) {
        arrow = search.sortDir === "desc" ? "↓" : "↑";
      }
      return {
        id: column.name,
        accessorKey: column.name,
        header: () => (
          <div className="ui-grid-head-cell">
            <button
              type="button"
              className="ui-sort"
              onClick={() => {
                onSort(column.name);
              }}
            >
              <span>{column.name}</span>
              <span className="ui-soft">{arrow}</span>
            </button>
            <input
              className="ui-input"
              value={tableFilterValue(search, column.name)}
              onChange={(event) => {
                onFilter(column.name, event.target.value);
              }}
              placeholder="filter"
            />
          </div>
        ),
        cell: (context) => <div className="ui-cell">{renderCell(context.getValue() as StudioCellValue)}</div>,
      };
    });
  }, [data.columns, onFilter, onSort, search]);

  const table = useReactTable<Record<string, StudioCellValue>>({
    data: data.rows,
    columns,
    getCoreRowModel: getCoreRowModel(),
  });

  return (
    <section className="ui-surface">
      <div className="ui-grid-wrap">
        <table className="ui-grid">
          <thead>
            {table.getHeaderGroups().map((group) => (
              <tr key={group.id}>
                {group.headers.map((header) => (
                  <th key={header.id}>{header.isPlaceholder ? null : flexRender(header.column.columnDef.header, header.getContext())}</th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.length === 0 ? (
              <tr>
                <td colSpan={Math.max(1, data.columns.length)} className="ui-empty">No rows matched this filter.</td>
              </tr>
            ) : (
              table.getRowModel().rows.map((row) => (
                <tr key={row.id}>
                  {row.getVisibleCells().map((cell) => (
                    <td key={cell.id}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>
                  ))}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
      <footer className="ui-grid-footer">
        <p className="ui-muted">
          {data.totalRows} rows · page {data.page} / {data.totalPages}
        </p>
        <div className="ui-grid-controls">
          <select
            className="ui-select"
            value={data.pageSize}
            onChange={(event) => {
              onPageSize(Number.parseInt(event.target.value, 10));
            }}
          >
            <option value={25}>25</option>
            <option value={50}>50</option>
            <option value={100}>100</option>
          </select>
          <button type="button" className="ui-btn-ghost" disabled={data.page <= 1} onClick={() => onPage(data.page - 1)}>
            Prev
          </button>
          <button
            type="button"
            className="ui-btn-ghost"
            disabled={data.page >= data.totalPages}
            onClick={() => onPage(data.page + 1)}
          >
            Next
          </button>
        </div>
      </footer>
    </section>
  );
}
