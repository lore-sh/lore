declare const LORE_BUILD_VERSION: string | undefined;

export const VERSION = typeof LORE_BUILD_VERSION === "string" ? LORE_BUILD_VERSION : "dev";
