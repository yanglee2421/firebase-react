import { QueryErrorResetBoundary } from "@tanstack/react-query";
import React from "react";
import { ErrorBoundary } from "react-error-boundary";
import { Charts } from "./Charts";

export function Component() {
  return (
    <>
      <title>Charts</title>
      <QueryErrorResetBoundary>
        {({ reset }) => {
          return (
            <ErrorBoundary
              onReset={reset}
              fallbackRender={({ error, resetErrorBoundary }) => {
                return (
                  <div>
                    <p>{error.message}</p>
                    <button onClick={resetErrorBoundary}>reset</button>
                  </div>
                );
              }}
            >
              <React.Suspense>
                <Charts />
              </React.Suspense>
            </ErrorBoundary>
          );
        }}
      </QueryErrorResetBoundary>
    </>
  );
}
