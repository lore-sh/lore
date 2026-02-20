import { Link, Outlet, useLocation } from "@tanstack/react-router";

function navClass(current: string, path: string): string {
  const active = current === path || (path !== "/" && current.startsWith(path));
  return active
    ? "rounded-full bg-slate-900 px-4 py-2 text-sm font-semibold text-amber-100"
    : "rounded-full px-4 py-2 text-sm font-medium text-slate-700 hover:bg-amber-100";
}

export function AppLayout() {
  const location = useLocation();
  const path = location.pathname;

  return (
    <div className="min-h-screen bg-[radial-gradient(1200px_500px_at_10%_-10%,#fef3c7,transparent),radial-gradient(1000px_600px_at_100%_0%,#bae6fd,transparent),#f8fafc] text-slate-900">
      <header className="border-b border-slate-200/80 bg-white/80 backdrop-blur">
        <div className="mx-auto flex w-full max-w-6xl items-center justify-between gap-4 px-6 py-4">
          <div className="flex items-center gap-3">
            <div className="h-8 w-8 rounded-full bg-slate-900" />
            <div>
              <p className="text-xs uppercase tracking-[0.2em] text-slate-500">toss</p>
              <p className="font-semibold">studio</p>
            </div>
          </div>
          <nav className="flex items-center gap-1 rounded-full bg-white p-1 shadow-sm ring-1 ring-slate-200">
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

      <main className="mx-auto w-full max-w-6xl px-6 py-8">
        <Outlet />
      </main>
    </div>
  );
}
