// Redux Toolkit Imports
import { combineReducers, configureStore } from "@reduxjs/toolkit";

// Persist Imports
import {
  persistStore,
  persistReducer,
  FLUSH,
  REHYDRATE,
  PAUSE,
  PERSIST,
  PURGE,
  REGISTER,
} from "redux-persist";
import storage from "redux-persist/lib/storage";
import session from "redux-persist/lib/storage/session";

// Slice Imports
import { sliceLoginLocal } from "./slice-login-local";
import { sliceLoginSession } from "./slice-login-session";

// Create Reducer
const rootReducer = combineReducers({
  [sliceLoginLocal.name]: sliceLoginLocal.reducer,
  [sliceLoginSession.name]: persistReducer(
    {
      key: sliceLoginSession.name,
      storage: session,
      blacklist: [],
    },
    sliceLoginSession.reducer
  ),
});

// Create Persisted Reducer
const reducer = persistReducer(
  {
    key: import.meta.env.VITE_REDUX_PERSISTER_KEY,
    version: 1,
    storage,
    blacklist: [sliceLoginSession.name],
  },
  rootReducer
);

// Create Store
export const store = configureStore({
  reducer,
  middleware(getMiddleWare) {
    return getMiddleWare({
      serializableCheck: {
        ignoredActions: [FLUSH, REHYDRATE, PAUSE, PERSIST, PURGE, REGISTER],
      },
    });
  },
});

// Create Persisted Store
export const persistor = persistStore(store);

// ** Types
export type RootState = ReturnType<typeof store.getState>;
export type AppDispatch = typeof store.dispatch;
