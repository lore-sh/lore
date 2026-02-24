import { Link } from "@tanstack/react-router";
import type { tableOverview } from "@toss/core";
import { formatRelativeTime } from "../lib/time";

type TableOverview = ReturnType<typeof tableOverview>[number];

interface TableRowProps {
  table: TableOverview;
}

export function TableRow({ table }: TableRowProps) {
  return (
    <Link
      to="/tables/$name"
      params={{ name: table.name }}
      search={{ tab: "data", page: 1, pageSize: 50 }}
      className="ui-table-list-row"
    >
      <span className="ui-table-name">{table.name}</span>
      <span className="ui-table-stat">{table.rowCount} rows · {table.columnCount} cols</span>
      <span className="ui-table-time">{table.lastUpdatedAt ? formatRelativeTime(table.lastUpdatedAt) : "—"}</span>
    </Link>
  );
}
