import { Link, Outlet, useLocation } from "@tanstack/react-router";

function navClass(current: string, path: string): string {
  const active = current === path || (path !== "/" && current.startsWith(path));
  return active ? "ui-nav-link ui-nav-link-active" : "ui-nav-link";
}

export function AppLayout() {
  const location = useLocation();
  const path = location.pathname;

  return (
    <div className="ui-shell">
      <header className="ui-header">
        <div className="ui-container flex items-center justify-between gap-4 px-6 py-4">
          <div className="flex items-center gap-3">
            <div className="h-8 w-8 rounded-full bg-brand" />
            <div>
              <p className="text-xs uppercase tracking-[0.2em] text-fg-soft">toss</p>
              <p className="font-semibold">studio</p>
            </div>
          </div>
          <nav className="ui-nav">
            <Link to="/" className={navClass(path, "/")}>
              Dashboard
            </Link>
            <Link to="/schema" className={navClass(path, "/schema")}>
              Schema
            </Link>
            <Link to="/history" className={navClass(path, "/history")}>
              History
            </Link>
          </nav>
        </div>
      </header>

      <main className="ui-container px-6 py-8">
        <Outlet />
      </main>
    </div>
  );
}
