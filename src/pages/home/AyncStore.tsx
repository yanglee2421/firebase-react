import localforage from "localforage";
import React from "react";
import { create } from "zustand";
import { createJSONStorage, persist } from "zustand/middleware";
import { timeout } from "@/utils/timeout";

export function AsyncStore() {
  const value = useAsyncStore((store) => store.value);
  const setValue = useAsyncStore((store) => store.setValue);

  const hasHydrated = React.useSyncExternalStore(
    (onStoreChange) => useAsyncStore.persist.onFinishHydration(onStoreChange),
    () => useAsyncStore.persist.hasHydrated(),
    () => false,
  );

  return (
    <input
      disabled={!hasHydrated}
      value={value}
      onChange={(evt) => {
        setValue(evt.target.value);
      }}
      className="block w-full focus:border-blue-500 focus:ring-blue-500 disabled:border-slate-200 disabled:bg-slate-50 disabled:text-slate-500 disabled:shadow-none"
    />
  );
}

const useAsyncStore = create(
  persist<AsyncStore>(
    (set, get) => {
      return {
        value: "",
        setValue(updater) {
          set({
            value:
              typeof updater === "function" ? updater(get().value) : updater,
          });
        },
      };
    },
    {
      name: "useAsyncStore",
      storage: createJSONStorage(() => {
        return {
          async getItem(name) {
            await timeout(1000 * 2);
            return localforage.getItem(name);
          },
          async setItem(name, value) {
            await timeout(1000 * 2);
            return localforage.setItem(name, value);
          },
          async removeItem(name) {
            return localforage.removeItem(name);
          },
        };
      }),
    },
  ),
);

type AsyncStore = {
  value: string;
  setValue: React.Dispatch<React.SetStateAction<string>>;
};
