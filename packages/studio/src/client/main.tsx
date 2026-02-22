import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  RouterProvider,
  createRootRouteWithContext,
  createRoute,
  createRouter,
} from "@tanstack/react-router";
import { createRoot } from "react-dom/client";
import { AppLayout } from "./components/layout";
import { historyQueryOptions, statusQueryOptions, tableDataQueryOptions, tableHistoryQueryOptions, tableSchemaQueryOptions, tablesQueryOptions } from "./lib/queries";
import { tableFilters, validateTableSearch } from "./lib/table-search";
import { validateTimelineSearch } from "./lib/timeline-search";
import { DashboardPage } from "./routes/index";
import { TablePage } from "./routes/table.$name";
import { TimelinePage } from "./routes/timeline";
import "./styles.css";

interface RouterContext {
  queryClient: QueryClient;
}

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
});

const rootRoute = createRootRouteWithContext<RouterContext>()({
  component: AppLayout,
  pendingComponent: () => <p className="ui-muted">Loading...</p>,
  errorComponent: ({ error }) => (
    <p className="ui-error">{error instanceof Error ? error.message : String(error)}</p>
  ),
});

const dashboardRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  loader: async ({ context }) => {
    await Promise.all([
      context.queryClient.ensureQueryData(statusQueryOptions()),
      context.queryClient.ensureQueryData(tablesQueryOptions()),
      context.queryClient.ensureQueryData(historyQueryOptions({ limit: 20, page: 1 })),
    ]);
  },
  component: DashboardPage,
});

const tableRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/tables/$name",
  validateSearch: validateTableSearch,
  loaderDeps: ({ search }) => search,
  loader: async ({ context, params, deps }) => {
    if (deps.tab === "data") {
      await context.queryClient.ensureQueryData(
        tableDataQueryOptions(params.name, {
          page: deps.page,
          pageSize: deps.pageSize,
          sortBy: deps.sortBy,
          sortDir: deps.sortDir,
          filters: tableFilters(deps),
        }),
      );
      return;
    }

    if (deps.tab === "schema") {
      await context.queryClient.ensureQueryData(tableSchemaQueryOptions(params.name));
      return;
    }

    await context.queryClient.ensureQueryData(tableHistoryQueryOptions(params.name, 50));
  },
  component: TablePage,
});

const timelineRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/timeline",
  validateSearch: validateTimelineSearch,
  loaderDeps: ({ search }) => search,
  loader: async ({ context, deps }) => {
    await Promise.all([
      context.queryClient.ensureQueryData(tablesQueryOptions()),
      context.queryClient.ensureQueryData(
        historyQueryOptions({
          limit: 50,
          page: deps.page,
          kind: deps.kind === "all" ? undefined : deps.kind,
          table: deps.table,
        }),
      ),
    ]);
  },
  component: TimelinePage,
});

const routeTree = rootRoute.addChildren([dashboardRoute, tableRoute, timelineRoute]);

const router = createRouter({
  routeTree,
  context: {
    queryClient,
  },
  defaultPreload: "intent",
});

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}

const root = document.getElementById("root");
if (!root) {
  throw new Error("Root element #root not found");
}

if (!document.documentElement.dataset.theme) {
  document.documentElement.dataset.theme = "light";
}

createRoot(root).render(
  <QueryClientProvider client={queryClient}>
    <RouterProvider router={router} />
  </QueryClientProvider>,
);
