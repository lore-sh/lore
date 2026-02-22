import type { ReactNode } from "react";
import { resolveQueryView, type QuerySnapshot } from "../lib/query-state";

interface QueryBoundaryProps<T> {
  query: QuerySnapshot<T>;
  loadingLabel: string;
  staleErrorLabel?: string;
  children: (data: T) => ReactNode;
}

export function QueryBoundary<T>({ query, loadingLabel, staleErrorLabel, children }: QueryBoundaryProps<T>) {
  const view = resolveQueryView(query);
  if (view.kind === "loading") {
    return <p className="ui-muted">{loadingLabel}</p>;
  }
  if (view.kind === "error") {
    return <p className="ui-error">{view.message}</p>;
  }

  return (
    <>
      {staleErrorLabel && view.refetchErrorMessage ? (
        <p className="ui-warn">{staleErrorLabel}: {view.refetchErrorMessage}</p>
      ) : null}
      {children(view.data)}
    </>
  );
}
