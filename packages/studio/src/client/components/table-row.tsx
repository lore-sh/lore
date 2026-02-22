import { Link } from "@tanstack/react-router";
import type { StudioTableSummary } from "@toss/core";
import { formatRelativeTime } from "../lib/time";

interface TableRowProps {
  table: StudioTableSummary;
}

export function TableRow({ table }: TableRowProps) {
  return (
    <li className="ui-table-list-row">
      <Link
        to="/tables/$name"
        params={{ name: table.name }}
        search={{ tab: "data", page: 1, pageSize: 50 }}
        className="ui-table-name"
      >
        {table.name}
      </Link>
      <span className="ui-muted">{table.rowCount} rows</span>
      <span className="ui-muted">{table.columnCount} cols</span>
      <span className="ui-soft">{table.lastUpdatedAt ? formatRelativeTime(table.lastUpdatedAt) : "never"}</span>
    </li>
  );
}
