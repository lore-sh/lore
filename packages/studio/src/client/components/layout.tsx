import { Link, Outlet, useLocation } from "@tanstack/react-router";

function navClass(active: boolean): string {
  return active ? "ui-nav-link ui-nav-link-active" : "ui-nav-link";
}

export function AppLayout() {
  const location = useLocation();
  const path = location.pathname;
  const isOverview = path === "/" || path.startsWith("/tables");
  const isTimeline = path.startsWith("/timeline");

  return (
    <div className="ui-shell">
      <header className="ui-header">
        <div className="ui-page ui-header-inner">
          <p className="ui-brand">Lore Studio</p>
          <nav className="ui-nav" aria-label="Studio sections">
            <Link to="/" className={navClass(isOverview)}>Overview</Link>
            <Link to="/timeline" search={{ page: 1, kind: "all" }} className={navClass(isTimeline)}>
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
