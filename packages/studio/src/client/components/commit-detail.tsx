import { useMutation, useQuery, useQueryClient, type QueryKey } from "@tanstack/react-query";
import type { revert } from "@lore/core";
import { useState } from "react";
import { QueryBoundary } from "./query-boundary";
import { revertCommitById } from "../lib/api";
import {
  renderOperationSyntax,
  renderPkLabel,
  renderRowEffectLines,
  renderSchemaEffectLine,
} from "../lib/commit-render";
import { commitDetailQueryOptions } from "../lib/queries";

const REVERT_INVALIDATION_KEYS = new Set([
  "history", "history-detail", "tables", "status", "table-data", "table-history", "table-schema",
]);

function shouldInvalidateAfterRevert(queryKey: QueryKey): boolean {
  return typeof queryKey[0] === "string" && REVERT_INVALIDATION_KEYS.has(queryKey[0]);
}

function diffKindClass(kind: "add" | "remove" | "neutral"): string {
  switch (kind) {
    case "add":
      return "ui-diff-add";
    case "remove":
      return "ui-diff-remove";
    case "neutral":
      return "ui-diff-neutral";
  }
}

type RevertConflict = Extract<ReturnType<typeof revert>, { ok: false }>["conflicts"][number];

interface CommitDetailProps {
  commitId: string;
  enableRevert?: boolean;
}

function ConflictList({ conflicts }: { conflicts: RevertConflict[] }) {
  if (conflicts.length === 0) {
    return null;
  }
  return (
    <div className="ui-surface-sub">
      <p className="ui-label">Conflicts</p>
      <ul className="ui-list-tight">
        {conflicts.map((conflict, index) => (
          <li key={`${conflict.table}-${index}`} className="ui-error">
            {conflict.table}: {conflict.reason}
          </li>
        ))}
      </ul>
    </div>
  );
}

export function CommitDetail({ commitId, enableRevert = false }: CommitDetailProps) {
  const detail = useQuery(commitDetailQueryOptions(commitId));
  const queryClient = useQueryClient();
  const [conflicts, setConflicts] = useState<RevertConflict[]>([]);
  const [success, setSuccess] = useState<string | null>(null);
  const [revertError, setRevertError] = useState<string | null>(null);

  const mutation = useMutation({
    mutationFn: () => revertCommitById(commitId),
    onMutate: () => {
      setRevertError(null);
      setSuccess(null);
      setConflicts([]);
    },
    onSuccess: async (result) => {
      if (result.ok) {
        setSuccess(`Reverted with commit ${result.revertCommit.commitId.slice(0, 12)}`);
        await queryClient.invalidateQueries({
          predicate: (query) => shouldInvalidateAfterRevert(query.queryKey),
        });
        return;
      }
      setConflicts(result.conflicts);
    },
    onError: (error) => {
      const message = error instanceof Error ? error.message : String(error);
      setRevertError(message);
      setSuccess(null);
      setConflicts([]);
    },
  });

  return (
    <QueryBoundary query={detail} loadingLabel="Loading detail..." staleErrorLabel="Failed to refresh detail, showing cached result">
      {(detailData) => {
        const commit = detailData.commit;
        return (
          <div className="ui-commit-detail">
            <section>
              <p className="ui-label">Operations</p>
              {detailData.operations.length === 0 ? (
                <p className="ui-soft">No operations</p>
              ) : (
                <div className="ui-code-block">
                  {detailData.operations.map((operation, index) => (
                    <div key={`${operation.type}-${index}`} className="ui-code-line">
                      {renderOperationSyntax(operation)}
                    </div>
                  ))}
                </div>
              )}
            </section>

            <section>
              <p className="ui-label">Effects</p>
              <div className="ui-stack-2">
                {detailData.effects.rows.map((effect, index) => (
                  <div key={`${effect.tableName}-${index}`} className="ui-effect-block">
                    <p className="ui-soft">
                      {effect.tableName}
                      {renderPkLabel(effect.pk).length > 0 ? ` · ${renderPkLabel(effect.pk)}` : ""}
                    </p>
                    <div className="ui-diff">
                      {renderRowEffectLines(effect).map((line, lineIndex) => (
                        <div key={lineIndex} className={diffKindClass(line.kind)}>
                          {line.text}
                        </div>
                      ))}
                    </div>
                  </div>
                ))}

                {detailData.effects.schemas.map((effect, index) => {
                  const line = renderSchemaEffectLine(effect);
                  return (
                    <div key={`${effect.tableName}-${index}`} className={diffKindClass(line.kind)}>
                      {line.text}
                    </div>
                  );
                })}
              </div>
            </section>

            {enableRevert && commit.revertible === 1 ? (
              <div className="ui-stack-2">
                <button
                  type="button"
                  className="ui-btn-ghost"
                  disabled={mutation.isPending}
                  onClick={() => {
                    const confirmed = window.confirm(`Revert commit ${commit.commitId.slice(0, 12)}: ${commit.message}?`);
                    if (!confirmed) {
                      return;
                    }
                    mutation.mutate();
                  }}
                >
                  {mutation.isPending ? "Reverting..." : "Revert this commit"}
                </button>
                {revertError ? <p className="ui-error">{revertError}</p> : null}
                {success ? <p className="ui-ok">{success}</p> : null}
                <ConflictList conflicts={conflicts} />
              </div>
            ) : null}
          </div>
        );
      }}
    </QueryBoundary>
  );
}
