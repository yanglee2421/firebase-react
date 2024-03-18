import { QueryClient } from "@tanstack/react-query";

export const nonPersistentQueryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 1000 * 60 * 5,
      retry: false,
    },
  },
});
