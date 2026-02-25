import { Link, Outlet, useLocation } from "@tanstack/react-router";

function navClass(active: boolean): string {
  return active ? "ui-nav-link ui-nav-link-active" : "ui-nav-link";
}

function LoreIcon() {
  return (
    <svg width="22" height="22" viewBox="0 0 512 512" fill="none">
      <defs>
        <linearGradient id="lore-base" x1="104" y1="84" x2="418" y2="452" gradientUnits="userSpaceOnUse">
          <stop offset="0" stopColor="#C6D7FF" />
          <stop offset=".38" stopColor="#5A77F4" />
          <stop offset=".72" stopColor="#2B46BD" />
          <stop offset="1" stopColor="#241A74" />
        </linearGradient>
        <linearGradient id="lore-top" x1="133" y1="95" x2="340" y2="307" gradientUnits="userSpaceOnUse">
          <stop offset="0" stopColor="#E6EDFF" />
          <stop offset=".55" stopColor="#7CA0FF" />
          <stop offset="1" stopColor="#3950D0" />
        </linearGradient>
        <linearGradient id="lore-band" x1="115" y1="245" x2="393" y2="287" gradientUnits="userSpaceOnUse">
          <stop offset="0" stopColor="#7D73D9" />
          <stop offset=".5" stopColor="#5B8CFF" />
          <stop offset="1" stopColor="#6543BE" />
        </linearGradient>
        <linearGradient id="lore-deep" x1="255" y1="306" x2="255" y2="484" gradientUnits="userSpaceOnUse">
          <stop offset="0" stopColor="#2A3FA7" />
          <stop offset="1" stopColor="#140E4B" />
        </linearGradient>
      </defs>
      <path d="M256 34L390 112L469 201L468 357L391 429L256 507L122 428L44 357L43 201L122 112Z" fill="url(#lore-base)" />
      <path d="M256 34L390 112L443 188L256 307L69 188L122 112Z" fill="url(#lore-top)" />
      <path d="M99 248L255 307L413 248L393 286L255 336L118 286Z" fill="url(#lore-band)" />
      <path d="M69 188L256 307L99 248Z" fill="#4E62CE" fillOpacity=".6" />
      <path d="M443 188L256 307L413 248Z" fill="#2942B3" fillOpacity=".7" />
      <path d="M255 307L393 286L391 429L255 507V307Z" fill="url(#lore-deep)" />
      <path d="M255 307L118 286L122 428L255 507V307Z" fill="#1F2686" />
    </svg>
  );
}

function OverviewIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <rect x="3" y="3" width="7" height="7" rx="1" />
      <rect x="14" y="3" width="7" height="7" rx="1" />
      <rect x="3" y="14" width="7" height="7" rx="1" />
      <rect x="14" y="14" width="7" height="7" rx="1" />
    </svg>
  );
}

function TimelineIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="12" r="10" />
      <polyline points="12 6 12 12 16 14" />
    </svg>
  );
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
          <Link to="/" className="ui-brand">
            <LoreIcon />
            <span>Lore Studio</span>
          </Link>
          <nav className="ui-nav" aria-label="Studio sections">
            <Link to="/" className={navClass(isOverview)}>
              <OverviewIcon />
              Overview
            </Link>
            <Link to="/timeline" search={{ page: 1, kind: "all" }} className={navClass(isTimeline)}>
              <TimelineIcon />
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
