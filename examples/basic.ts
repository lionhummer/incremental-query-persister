import { createIncrementalPersister, MemoryRowStore } from "../src/index.js";
const store = new MemoryRowStore();
const persister = createIncrementalPersister({ store });
persister.persistClient({
  timestamp: Date.now(),
  buster: "demo-v1",
  clientState: {
    mutations: [],
    queries: [
      {
        queryHash: "settings",
        queryKey: ["settings"],
        state: { data: { theme: "dark" }, dataUpdatedAt: 1, status: "success" },
      },
    ],
  },
});
await persister.flush();
console.log(await persister.restoreClient());
await persister.close();
