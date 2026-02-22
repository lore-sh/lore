import type { UseQueryResult } from "@tanstack/react-query";

export type QuerySnapshot<T> = Pick<
  UseQueryResult<T>,
  "data" | "error" | "isPending" | "isLoadingError" | "isRefetchError"
>;

export type QueryView<T> =
  | {
      kind: "data";
      data: T;
      refetchErrorMessage: string | null;
    }
  | {
      kind: "error";
      message: string;
    }
  | {
      kind: "loading";
    };

function queryErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

export function resolveQueryView<T>(query: QuerySnapshot<T>): QueryView<T> {
  if (query.data !== undefined) {
    return {
      kind: "data",
      data: query.data,
      refetchErrorMessage: query.isRefetchError ? queryErrorMessage(query.error) : null,
    };
  }

  if (query.error != null) {
    return {
      kind: "error",
      message: queryErrorMessage(query.error),
    };
  }

  return { kind: "loading" };
}
