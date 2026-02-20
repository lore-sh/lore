import { useQuery, useSuspenseQuery } from "@tanstack/react-query";
import { useState } from "react";
import { commitDetailQueryOptions, historyQueryOptions } from "../lib/queries";

function formatJson(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

function HistoryDetail(props: { commitId: string; open: boolean }) {
  const detail = useQuery({
    ...commitDetailQueryOptions(props.commitId),
    enabled: props.open,
  });

  if (!props.open) {
    return null;
  }
  if (detail.isPending) {
    return <p className="text-sm text-slate-600">Loading detail...</p>;
  }
  if (detail.isError) {
    const message = detail.error instanceof Error ? detail.error.message : String(detail.error);
    return <p className="text-sm text-red-700">{message}</p>;
  }
  if (!detail.data) {
    return null;
  }

  return (
    <div className="space-y-4">
      <section>
        <h3 className="mb-2 text-xs font-semibold uppercase tracking-[0.08em] text-slate-600">Operations</h3>
        <pre className="overflow-x-auto rounded-xl bg-slate-950 p-4 text-xs text-slate-100">
          {formatJson(detail.data.commit.operations)}
        </pre>
      </section>
      <section>
        <h3 className="mb-2 text-xs font-semibold uppercase tracking-[0.08em] text-slate-600">Effects</h3>
        <pre className="overflow-x-auto rounded-xl bg-slate-950 p-4 text-xs text-slate-100">
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
    return <p className="text-sm text-slate-500">No commits found.</p>;
  }

  function toggle(commitId: string): void {
    const opened = openIds[commitId] === true;
    setOpenIds((prev) => ({ ...prev, [commitId]: !opened }));
  }

  return (
    <section className="space-y-4">
      {history.map((commit) => {
        const opened = openIds[commit.commitId] === true;
        return (
          <article key={commit.commitId} className="overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">
            <button
              type="button"
              onClick={() => {
                toggle(commit.commitId);
              }}
              className="flex w-full items-center justify-between gap-4 px-5 py-4 text-left hover:bg-slate-50"
            >
              <div>
                <p className="font-mono text-xs text-slate-500">{commit.shortId}</p>
                <p className="mt-1 text-sm font-semibold text-slate-900">{commit.message}</p>
                <p className="text-xs text-slate-500">
                  {commit.createdAt} · {commit.kind}
                </p>
              </div>
              <span className="rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-700">
                {opened ? "Hide" : "Show"}
              </span>
            </button>
            {opened ? (
              <div className="border-t border-slate-100 bg-slate-50/50 px-5 py-4">
                <HistoryDetail commitId={commit.commitId} open={opened} />
              </div>
            ) : null}
          </article>
        );
      })}
    </section>
  );
}
