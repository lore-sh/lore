import { z } from "zod";

export const positiveIntSchema = z.coerce.number().int().min(1);

export const commitIdParamSchema = z.object({
  id: z.string().trim().min(1),
});

export const tableParamSchema = z.object({
  name: z.string().trim().min(1),
});

export function validationError(issues: z.ZodIssue[]): { code: string; message: string; details: z.ZodIssue[] } {
  return {
    code: "VALIDATION_ERROR",
    message: "Request validation failed",
    details: issues,
  };
}
