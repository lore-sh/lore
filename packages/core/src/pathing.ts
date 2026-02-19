function cwdPath(): string {
  return Bun.fileURLToPath(Bun.pathToFileURL("./"));
}

function isWindowsDriveAbsolute(pathLike: string): boolean {
  return /^[A-Za-z]:[\\/]/.test(pathLike);
}

function isUncAbsolute(pathLike: string): boolean {
  return pathLike.startsWith("\\\\") || pathLike.startsWith("//");
}

function isAbsolutePathLike(pathLike: string): boolean {
  return pathLike.startsWith("/") || isWindowsDriveAbsolute(pathLike) || isUncAbsolute(pathLike);
}

interface RootedPath {
  root: string;
  segments: string[];
}

function splitRoot(pathLike: string): RootedPath {
  if (isUncAbsolute(pathLike)) {
    const normalized = pathLike.replaceAll("\\", "/");
    const withoutPrefix = normalized.replace(/^\/+/, "");
    const [server = "", share = "", ...rest] = withoutPrefix.split("/");
    if (!server || !share) {
      return { root: "//", segments: withoutPrefix.split("/") };
    }
    return { root: `//${server}/${share}/`, segments: rest };
  }
  if (pathLike.startsWith("/")) {
    return { root: "/", segments: pathLike.slice(1).split("/") };
  }
  if (isWindowsDriveAbsolute(pathLike)) {
    const normalized = pathLike.replaceAll("\\", "/");
    return { root: `${normalized[0]}:/`, segments: normalized.slice(3).split("/") };
  }
  return { root: "", segments: pathLike.split("/") };
}

function normalizeFromRoot(root: string, segments: string[]): string {
  const stack: string[] = [];
  for (const raw of segments) {
    if (!raw || raw === ".") {
      continue;
    }
    if (raw === "..") {
      if (stack.length > 0) {
        stack.pop();
      } else if (!root) {
        stack.push("..");
      }
      continue;
    }
    stack.push(raw);
  }

  const body = stack.join("/");
  if (!root) {
    return body || ".";
  }
  if (root === "/") {
    return body ? `/${body}` : "/";
  }
  if (root.endsWith("/")) {
    return body ? `${root}${body}` : root;
  }
  return body ? `${root}/${body}` : root;
}

function normalizeAbsolute(pathLike: string): string {
  const rooted = splitRoot(pathLike);
  if (!rooted.root) {
    throw new Error(`Expected absolute path, got: ${pathLike}`);
  }
  return normalizeFromRoot(rooted.root, rooted.segments);
}

export function resolveFromCwd(pathLike: string): string {
  if (!pathLike) {
    return normalizeAbsolute(cwdPath());
  }
  if (isAbsolutePathLike(pathLike)) {
    return normalizeAbsolute(pathLike);
  }
  return normalizeAbsolute(`${normalizeAbsolute(cwdPath())}/${pathLike}`);
}

export function dirnameOf(pathLike: string): string {
  const resolved = resolveFromCwd(pathLike);
  if (resolved === "/" || /^[A-Za-z]:\/$/.test(resolved) || resolved === "//") {
    return resolved;
  }
  const idx = resolved.lastIndexOf("/");
  if (idx < 0) {
    return ".";
  }
  if (idx === 0) {
    return "/";
  }
  const dir = resolved.slice(0, idx);
  if (/^[A-Za-z]:$/.test(dir)) {
    return `${dir}/`;
  }
  return dir;
}

export function joinPath(base: string, ...parts: string[]): string {
  let current = resolveFromCwd(base);
  for (const part of parts) {
    if (!part) {
      continue;
    }
    if (isAbsolutePathLike(part)) {
      current = resolveFromCwd(part);
      continue;
    }
    current = normalizeAbsolute(`${current}/${part}`);
  }
  return current;
}
