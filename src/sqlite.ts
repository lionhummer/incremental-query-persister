import { DatabaseSync } from "node:sqlite";
import type { RowStore, RowSnapshot, RowChange } from "./index.js";
/** One durable, generation-fenced cache namespace. Use one writer per namespace. */
export class SqliteRowStore implements RowStore {
  private readonly db: DatabaseSync;
  constructor(
    path: string,
    private readonly namespace: string,
  ) {
    if (!namespace.trim()) throw new Error("namespace is required");
    this.db = new DatabaseSync(path);
    this.db.exec(
      "PRAGMA journal_mode=WAL;PRAGMA busy_timeout=5000; CREATE TABLE IF NOT EXISTS query_meta_v1(namespace TEXT PRIMARY KEY,generation INTEGER NOT NULL,buster TEXT,timestamp INTEGER NOT NULL);CREATE TABLE IF NOT EXISTS query_rows_v1(namespace TEXT NOT NULL,hash TEXT NOT NULL,body TEXT NOT NULL,PRIMARY KEY(namespace,hash));",
    );
    this.db
      .prepare(
        "INSERT INTO query_meta_v1 VALUES(?,0,NULL,0) ON CONFLICT DO NOTHING",
      )
      .run(namespace);
  }
  async read(): Promise<RowSnapshot> {
    this.db.exec("BEGIN");
    try {
      const meta = this.db
        .prepare(
          "SELECT generation,buster,timestamp FROM query_meta_v1 WHERE namespace=?",
        )
        .get(this.namespace)!;
      const rows = this.db
        .prepare(
          "SELECT hash,body FROM query_rows_v1 WHERE namespace=? ORDER BY hash",
        )
        .all(this.namespace)
        .map((r) => [String(r.hash), String(r.body)] as const);
      this.db.exec("COMMIT");
      return {
        generation: Number(meta.generation),
        buster: meta.buster === null ? null : String(meta.buster),
        timestamp: Number(meta.timestamp),
        rows,
      };
    } catch (e) {
      this.db.exec("ROLLBACK");
      throw e;
    }
  }
  async write(c: RowChange): Promise<boolean> {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const meta = this.db
        .prepare("SELECT generation FROM query_meta_v1 WHERE namespace=?")
        .get(this.namespace)!;
      if (Number(meta.generation) !== c.generation) {
        this.db.exec("ROLLBACK");
        return false;
      }
      if (c.reset)
        this.db
          .prepare("DELETE FROM query_rows_v1 WHERE namespace=?")
          .run(this.namespace);
      for (const h of c.removes)
        this.db
          .prepare("DELETE FROM query_rows_v1 WHERE namespace=? AND hash=?")
          .run(this.namespace, h);
      for (const [h, raw] of c.upserts)
        this.db
          .prepare(
            "INSERT INTO query_rows_v1 VALUES(?,?,?) ON CONFLICT(namespace,hash) DO UPDATE SET body=excluded.body",
          )
          .run(this.namespace, h, raw);
      this.db
        .prepare(
          "UPDATE query_meta_v1 SET buster=?,timestamp=? WHERE namespace=?",
        )
        .run(c.buster, c.timestamp, this.namespace);
      this.db.exec("COMMIT");
      return true;
    } catch (e) {
      this.db.exec("ROLLBACK");
      throw e;
    }
  }
  async clear(): Promise<void> {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      this.db
        .prepare("DELETE FROM query_rows_v1 WHERE namespace=?")
        .run(this.namespace);
      this.db
        .prepare(
          "UPDATE query_meta_v1 SET generation=generation+1,buster=NULL,timestamp=0 WHERE namespace=?",
        )
        .run(this.namespace);
      this.db.exec("COMMIT");
    } catch (e) {
      this.db.exec("ROLLBACK");
      throw e;
    }
  }
  close(): void {
    this.db.close();
  }
}
