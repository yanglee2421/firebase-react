import React from "react";
import { concatMap, from, interval, last, map, takeWhile } from "rxjs";

const scanner$ = interval(1000 * 10);

const app$ = scanner$.pipe(
  concatMap((barcode) => {
    const url = new URL("http://localhost:5003/api/getData");
    url.searchParams.set("param", barcode.toString());
    url.searchParams.set("type", "csbts");

    return from(fetch(url)).pipe(
      concatMap((res) => from(res.json())),
      concatMap((data) => {
        return interval(1000).pipe(
          takeWhile((res) => res < 3),
          last(),
          map((plc) => ({ data, plc })),
        );
      }),
    );
  }),
);

export const Component = () => {
  const [list, setList] = React.useState<Array<{}>>([]);

  React.useEffect(() => {
    const s = app$.subscribe((value) => {
      console.log("value", value);
      setList((prev) => [...prev, value]);
    });

    return () => {
      s.unsubscribe();
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
