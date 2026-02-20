import type { DatabaseOptions } from "@toss/core";

export interface StudioServerOptions extends DatabaseOptions {}

export interface StartStudioServerOptions extends StudioServerOptions {
  port?: number | undefined;
  host?: string | undefined;
  open?: boolean | undefined;
}
