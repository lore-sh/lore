export class TossError extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = "TossError";
    this.code = code;
  }
}

export function isTossError(error: unknown): error is TossError {
  return error instanceof TossError;
}
