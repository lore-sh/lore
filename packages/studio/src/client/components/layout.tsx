import { Link, Outlet, useLocation } from "@tanstack/react-router";

function navClass(active: boolean): string {
  return active ? "ui-nav-link ui-nav-link-active" : "ui-nav-link";
}

export function AppLayout() {
  const location = useLocation();
  const path = location.pathname;

  return (
    <div className="ui-shell">
      <header className="ui-header">
        <div className="ui-page ui-header-inner">
          <div>
            <p className="ui-brand">toss studio</p>
          </div>
          <nav className="ui-nav" aria-label="Studio sections">
            <Link to="/" className={navClass(path === "/")}>Overview</Link>
            <a href="/#tables" className={navClass(path.startsWith("/tables"))}>Tables</a>
            <Link to="/timeline" search={{ page: 1, kind: "all" }} className={navClass(path.startsWith("/timeline"))}>
              Timeline
            </Link>
          </nav>
        </div>
      </header>

      <main className="ui-page ui-main">
        <Outlet />
      </main>
    </div>
  );
}
