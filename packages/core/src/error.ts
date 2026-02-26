export type ErrorCategory = "client" | "not_found" | "conflict" | "internal";

const ERROR_CATALOG = {
  CONFIG: { category: "client", httpStatus: 400 },
  NOT_INITIALIZED: { category: "client", httpStatus: 400 },
  NOT_FOUND: { category: "not_found", httpStatus: 404 },
  INVALID_OPERATION: { category: "client", httpStatus: 400 },
  INVALID_IDENTIFIER: { category: "client", httpStatus: 400 },
  INVALID_JSON: { category: "client", httpStatus: 400 },
  INVALID_PLAN: { category: "client", httpStatus: 400 },
  INVALID_SQL: { category: "client", httpStatus: 400 },
  APPLY_FAILED: { category: "internal", httpStatus: 500 },
  UNSUPPORTED: { category: "client", httpStatus: 400 },
  NO_PRIMARY_KEY: { category: "client", httpStatus: 400 },
  REVERT_FAILED: { category: "internal", httpStatus: 500 },
  NOT_REVERTIBLE: { category: "client", httpStatus: 400 },
  ALREADY_REVERTED: { category: "conflict", httpStatus: 409 },
  SNAPSHOT_FAILED: { category: "internal", httpStatus: 500 },
  RECOVER_FAILED: { category: "internal", httpStatus: 500 },
  SYNC_NOT_CONFIGURED: { category: "client", httpStatus: 400 },
  SYNC_NON_FAST_FORWARD: { category: "conflict", httpStatus: 409 },
  SYNC_DIVERGED: { category: "conflict", httpStatus: 409 },
  IMMUTABLE_MIGRATION_EDITED: { category: "conflict", httpStatus: 409 },
  SYNC_AUTH_FAILED: { category: "client", httpStatus: 400 },
  SYNC_UNREACHABLE: { category: "internal", httpStatus: 500 },
  INTERNAL: { category: "internal", httpStatus: 500 },
} as const satisfies Record<
  string,
  {
    readonly category: ErrorCategory;
    readonly httpStatus: 400 | 404 | 409 | 500;
  }
>;
export type ErrorCode = keyof typeof ERROR_CATALOG;

export class CodedError extends Error {
  override readonly name = "CodedError";
  readonly code: ErrorCode;

  constructor(code: ErrorCode, message: string, options?: ErrorOptions) {
    super(message, options);
    this.code = code;
  }

  static is(error: unknown): error is CodedError {
    if (!(error instanceof Error) || error.name !== "CodedError") {
      return false;
    }
    return "code" in error && typeof error.code === "string" && Object.hasOwn(ERROR_CATALOG, error.code);
  }

  static hasCode<C extends ErrorCode>(
    error: unknown,
    code: C,
  ): error is CodedError & { code: C } {
    return CodedError.is(error) && error.code === code;
  }

  toJSON(): { code: ErrorCode; message: string } {
    return { code: this.code, message: this.message };
  }
}

export interface HttpProblem {
  type: string;
  title: string;
  status: number;
  detail: string;
  instance: string;
  code: ErrorCode;
}

export function httpStatusFromError(code: ErrorCode): 400 | 404 | 409 | 500 {
  return ERROR_CATALOG[code].httpStatus;
}

export function toHttpProblem(error: CodedError, instance: string): HttpProblem {
  return {
    type: `https://docs.getlore.sh/errors/${error.code.toLowerCase()}`,
    title: error.code,
    status: httpStatusFromError(error.code),
    detail: error.message,
    instance,
    code: error.code,
  };
}
