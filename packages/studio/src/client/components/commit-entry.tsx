import { useState } from "react";
import type { StudioHistoryEntry } from "@toss/core";
import { formatRelativeTime } from "../lib/time";
import { summarizeHistoryEntry } from "../lib/commit-render";
import { CommitDetail } from "./commit-detail";

interface CommitEntryProps {
  commit: StudioHistoryEntry;
  showAffectedTables?: boolean;
  expandable?: boolean;
  enableRevert?: boolean;
}

function affectedTablesLabel(commit: StudioHistoryEntry): string {
  if (commit.affectedTables.length === 0) {
    return "no tables";
  }
  if (commit.affectedTables.length <= 2) {
    return commit.affectedTables.join(", ");
  }
  const head = commit.affectedTables.slice(0, 2).join(", ");
  return `${head} +${commit.affectedTables.length - 2}`;
}

export function CommitEntry({
  commit,
  showAffectedTables = true,
  expandable = true,
  enableRevert = false,
}: CommitEntryProps) {
  const [open, setOpen] = useState(false);
  const metadata = [formatRelativeTime(commit.createdAt), commit.kind];
  if (showAffectedTables) {
    metadata.push(affectedTablesLabel(commit));
  }
  metadata.push(summarizeHistoryEntry(commit));

  return (
    <li className="ui-timeline-item">
      <div className="ui-timeline-dot" aria-hidden />
      <div className="ui-timeline-body">
        <button
          type="button"
          className="ui-entry-trigger"
          onClick={expandable ? () => setOpen((prev) => !prev) : undefined}
        >
          <p className="ui-entry-title">{commit.message}</p>
          <p className="ui-entry-meta">{metadata.join(" · ")}</p>
        </button>
        {open ? (
          <div className="ui-entry-detail">
            <CommitDetail commitId={commit.commitId} enableRevert={enableRevert} />
          </div>
        ) : null}
      </div>
    </li>
  );
}
