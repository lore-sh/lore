export interface StudioServerOptions {
  dbPath?: string | undefined;
}

export interface StartStudioServerOptions extends StudioServerOptions {
  port?: number | undefined;
  host?: string | undefined;
  open?: boolean | undefined;
}
