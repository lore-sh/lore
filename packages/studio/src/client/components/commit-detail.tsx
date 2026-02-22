import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { RevertConflict } from "@toss/core";
import { useState } from "react";
import { revertCommitById } from "../lib/api";
import {
  renderOperationLine,
  renderPkLabel,
  renderRowEffectLines,
  renderSchemaEffectLine,
} from "../lib/commit-render";
import { commitDetailQueryOptions } from "../lib/queries";

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
          predicate: (query) => {
            const key = query.queryKey[0];
            return (
              key === "history" ||
              key === "history-detail" ||
              key === "tables" ||
              key === "status" ||
              key === "table-data" ||
              key === "table-history" ||
              key === "table-schema"
            );
          },
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

  if (detail.isPending) {
    return <p className="ui-muted">Loading detail...</p>;
  }
  if (detail.isError) {
    const message = detail.error instanceof Error ? detail.error.message : String(detail.error);
    return <p className="ui-error">{message}</p>;
  }

  const commit = detail.data.commit;

  return (
    <div className="ui-commit-detail">
      <section>
        <p className="ui-label">Operations</p>
        {commit.operations.length === 0 ? (
          <p className="ui-soft">No operations</p>
        ) : (
          <ul className="ui-list-tight">
            {commit.operations.map((operation, index) => (
              <li key={`${operation.type}-${index}`} className="ui-mono ui-line">
                {renderOperationLine(operation)}
              </li>
            ))}
          </ul>
        )}
      </section>

      <section>
        <p className="ui-label">Effects</p>
        <div className="ui-stack-2">
          {detail.data.rowEffects.map((effect, index) => (
            <div key={`${effect.tableName}-${index}`} className="ui-effect-block">
              <p className="ui-soft">
                {effect.tableName}
                {renderPkLabel(effect.pk).length > 0 ? ` · ${renderPkLabel(effect.pk)}` : ""}
              </p>
              <div className="ui-diff">
                {renderRowEffectLines(effect).map((line, lineIndex) => (
                  <div key={lineIndex} className={line.kind === "add" ? "ui-diff-add" : "ui-diff-remove"}>
                    {line.text}
                  </div>
                ))}
              </div>
            </div>
          ))}

          {detail.data.schemaEffects.map((effect, index) => {
            const line = renderSchemaEffectLine(effect);
            const className = line.kind === "add" ? "ui-diff-add" : line.kind === "remove" ? "ui-diff-remove" : "ui-diff-neutral";
            return (
              <div key={`${effect.tableName}-${index}`} className={className}>
                {line.text}
              </div>
            );
          })}
        </div>
      </section>

      {enableRevert && commit.revertible ? (
        <div className="ui-stack-2">
          <button
            type="button"
            className="ui-btn-primary"
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
}
