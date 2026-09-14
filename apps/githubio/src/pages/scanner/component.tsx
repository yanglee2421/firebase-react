import React from "react";
import {
  concatMap,
  distinctUntilChanged,
  from,
  interval,
  map,
  of,
  Subject,
  switchMap,
  takeWhile,
  tap,
} from "rxjs";
import { fromFetch } from "rxjs/fetch";

const scanner$ = interval(1000 * 10).pipe(
  map(() => Date.now().toString()),
  distinctUntilChanged(),
);
const hmisReady = (barcode: string) => {
  const url = new URL("http://localhost:5003/api/getData");
  url.searchParams.set("param", barcode.toString());
  url.searchParams.set("type", "csbts");

  return fromFetch(url.href);
};
const plcActionReady = (data: unknown) => {
  if (data) {
    return of(null).pipe(
      tap(() => {
        console.log(data);
      }),
    );
  }

  return interval(1000).pipe(
    map(() => Math.floor(Math.random() * 10)),
    takeWhile((data) => data < 5),
  );
};
const inputWindow$ = new Subject();
const inputReady = (data: unknown) => {
  return inputWindow$.pipe(
    tap(() => {
      console.log(data);
    }),
  );
};
void plcActionReady;
const app$ = scanner$.pipe(
  concatMap((barcode) => {
    return hmisReady(barcode).pipe(
      switchMap((r) => from(r.json())),
      switchMap((data) => inputReady(data)),
    );
  }),
);

export const Component = () => {
  const [list] = React.useState<Array<{}>>([]);

  React.useEffect(() => {
    const sub = app$.subscribe();

    return () => {
      sub.unsubscribe();
    };
  }, []);

  return (
    <div>
      <ul>
        {list.map((i, index) => (
          <li key={index}>{JSON.stringify(i)}</li>
        ))}
      </ul>
    </div>
  );
};
