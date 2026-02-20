import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  RouterProvider,
  createRootRouteWithContext,
  createRoute,
  createRouter,
} from "@tanstack/react-router";
import { createRoot } from "react-dom/client";
import { AppLayout } from "./components/layout";
import { HistoryPage } from "./routes/history";
import { DashboardPage } from "./routes/index";
import { SchemaPage } from "./routes/schema";
import { TablePage } from "./routes/table.$name";
import { historyQueryOptions, schemaQueryOptions, statusQueryOptions, tableDataQueryOptions, tablesQueryOptions } from "./lib/queries";
import { validateTableSearch } from "./lib/table-search";
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
  pendingComponent: () => <p className="text-sm text-fg-muted">Loading...</p>,
  errorComponent: ({ error }) => (
    <p className="text-sm text-danger">{error instanceof Error ? error.message : String(error)}</p>
  ),
});

const dashboardRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  loader: async ({ context }) => {
    await Promise.all([
      context.queryClient.ensureQueryData(tablesQueryOptions()),
      context.queryClient.ensureQueryData(statusQueryOptions()),
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
    await context.queryClient.ensureQueryData(tableDataQueryOptions(params.name, deps));
  },
  component: TablePage,
});

const schemaRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/schema",
  loader: async ({ context }) => {
    await context.queryClient.ensureQueryData(schemaQueryOptions());
  },
  component: SchemaPage,
});

const historyRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/history",
  loader: async ({ context }) => {
    await context.queryClient.ensureQueryData(historyQueryOptions(200));
  },
  component: HistoryPage,
});

const routeTree = rootRoute.addChildren([dashboardRoute, tableRoute, schemaRoute, historyRoute]);

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
