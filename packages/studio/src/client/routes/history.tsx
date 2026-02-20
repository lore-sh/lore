import { useQuery, useSuspenseQuery } from "@tanstack/react-query";
import { useState } from "react";
import { commitDetailQueryOptions, historyQueryOptions } from "../lib/queries";

function formatJson(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

function HistoryDetail(props: { commitId: string }) {
  const detail = useQuery(commitDetailQueryOptions(props.commitId));

  if (detail.isPending) {
    return <p className="text-sm text-fg-muted">Loading detail...</p>;
  }
  if (detail.isError) {
    const message = detail.error instanceof Error ? detail.error.message : String(detail.error);
    return <p className="text-sm text-danger">{message}</p>;
  }
  if (!detail.data) {
    return null;
  }

  return (
    <div className="space-y-4">
      <section>
        <h3 className="mb-2 text-xs font-semibold uppercase tracking-[0.08em] text-fg-muted">Operations</h3>
        <pre className="ui-code-block">
          {formatJson(detail.data.commit.operations)}
        </pre>
      </section>
      <section>
        <h3 className="mb-2 text-xs font-semibold uppercase tracking-[0.08em] text-fg-muted">Effects</h3>
        <pre className="ui-code-block">
          {formatJson({
            row: detail.data.rowEffects,
            schema: detail.data.schemaEffects,
          })}
        </pre>
      </section>
    </div>
  );
}

export function HistoryPage() {
  const { data: history } = useSuspenseQuery(historyQueryOptions(200));
  const [openIds, setOpenIds] = useState<Record<string, boolean>>({});
  if (history.length === 0) {
    return <p className="text-sm text-fg-soft">No commits found.</p>;
  }

  function toggle(commitId: string): void {
    setOpenIds((prev) => ({ ...prev, [commitId]: !prev[commitId] }));
  }

  return (
    <section className="space-y-4">
      {history.map((commit) => {
        const opened = openIds[commit.commitId] === true;
        return (
          <article key={commit.commitId} className="ui-surface">
            <button
              type="button"
              onClick={() => {
                toggle(commit.commitId);
              }}
              className="flex w-full items-center justify-between gap-4 px-5 py-4 text-left hover:bg-bg-subtle"
            >
              <div>
                <p className="font-mono text-xs text-fg-soft">{commit.shortId}</p>
                <p className="mt-1 text-sm font-semibold text-fg">{commit.message}</p>
                <p className="text-xs text-fg-soft">
                  {commit.createdAt} · {commit.kind}
                </p>
              </div>
              <span className="ui-badge">
                {opened ? "Hide" : "Show"}
              </span>
            </button>
            {opened ? (
              <div className="ui-surface-detail">
                <HistoryDetail commitId={commit.commitId} />
              </div>
            ) : null}
          </article>
        );
      })}
    </section>
  );
}
