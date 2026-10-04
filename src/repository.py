import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone

from .domain import ConflictError, NotFoundError


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class SQLiteRepository:
    def __init__(self, path):
        self.path = str(path)
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self):
        with self._connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_entities_kind_status
                    ON entities(kind, status);
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_role TEXT NOT NULL,
                    action TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_entity
                    ON audit_log(entity_id, id);
                CREATE TABLE IF NOT EXISTS idempotency (
                    actor_id TEXT NOT NULL,
                    idem_key TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(actor_id, idem_key)
                );
                CREATE TABLE IF NOT EXISTS sync_batches (
                    batch_id TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    digest TEXT NOT NULL,
                    records TEXT NOT NULL,
                    result TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
            """)

    @staticmethod
    def _entity_from_row(row):
        return {
            "id": row["id"],
            "kind": row["kind"],
            "status": row["status"],
            "version": int(row["version"]),
            "data": json.loads(row["data"]),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    @staticmethod
    def _batch_from_row(row):
        return {
            "batch_id": row["batch_id"],
            "source_id": row["source_id"],
            "status": row["status"],
            "digest": row["digest"],
            "records": json.loads(row["records"]),
            "result": json.loads(row["result"]) if row["result"] else None,
            "error": row["error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    @contextmanager
    def transaction(self):
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def create_entity(self, entity_id, kind, status, data, actor_id, conn=None):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        sql = (
            "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
            "VALUES (?, ?, ?, 1, ?, ?, ?, ?)"
        )
        params = (entity_id, kind, status, payload, actor_id, now, now)
        if conn is None:
            with self._connect() as connection:
                connection.execute(sql, params)
        else:
            conn.execute(sql, params)
        return self.get_entity(entity_id, conn=conn)

    def get_entity(self, entity_id, conn=None):
        sql = "SELECT * FROM entities WHERE id = ?"
        if conn is None:
            with self._connect() as connection:
                row = connection.execute(sql, (entity_id,)).fetchone()
        else:
            row = conn.execute(sql, (entity_id,)).fetchone()
        return self._entity_from_row(row) if row else None

    def list_entities(self, kind=None, status=None, conn=None):
        clauses = []
        params = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = "SELECT * FROM entities" + where + " ORDER BY created_at, id"
        if conn is None:
            with self._connect() as connection:
                rows = connection.execute(sql, params).fetchall()
        else:
            rows = conn.execute(sql, params).fetchall()
        return [self._entity_from_row(row) for row in rows]

    def find_entities(self, kind, field, value, conn=None):
        entities = self.list_entities(kind=kind, conn=conn)
        if field == "*":
            return entities
        return [
            entity
            for entity in entities
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def update_entity(self, entity_id, expected_version, status, data, conn=None):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        sql_select = "SELECT version FROM entities WHERE id = ?"
        sql_update = (
            "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
            "WHERE id = ? AND version = ?"
        )
        if conn is None:
            connection = self._connect()
            close = True
            try:
                connection.execute("BEGIN IMMEDIATE")
            except Exception:
                connection.close()
                raise
        else:
            connection = conn
            close = False
        try:
            row = connection.execute(sql_select, (entity_id,)).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + entity_id)
            current_version = int(row["version"])
            if expected_version is not None and current_version != int(expected_version):
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_version, current_version)
                )
            connection.execute(
                sql_update,
                (status, payload, now, entity_id, current_version),
            )
            if close:
                connection.commit()
        except Exception:
            if close:
                connection.rollback()
            raise
        finally:
            if close:
                connection.close()
        return self.get_entity(entity_id, conn=conn)

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail, conn=None):
        sql = (
            "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )
        params = (
            entity_id,
            actor_id,
            actor_role,
            action,
            from_status,
            to_status,
            json.dumps(detail, ensure_ascii=False, sort_keys=True),
            utcnow(),
        )
        if conn is None:
            with self._connect() as connection:
                connection.execute(sql, params)
        else:
            conn.execute(sql, params)

    def list_audit(self, entity_id=None):
        with self._connect() as connection:
            if entity_id:
                rows = connection.execute(
                    "SELECT * FROM audit_log WHERE entity_id = ? ORDER BY id", (entity_id,)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM audit_log ORDER BY id").fetchall()
        return [
            {
                "id": row["id"],
                "entity_id": row["entity_id"],
                "actor_id": row["actor_id"],
                "actor_role": row["actor_role"],
                "action": row["action"],
                "from_status": row["from_status"],
                "to_status": row["to_status"],
                "detail": json.loads(row["detail"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def get_idempotency(self, actor_id, idem_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
                (actor_id, idem_key),
            ).fetchone()
        return row["entity_id"] if row else None

    def save_idempotency(self, actor_id, idem_key, entity_id):
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO idempotency(actor_id, idem_key, entity_id, created_at) "
                "VALUES (?, ?, ?, ?)",
                (actor_id, idem_key, entity_id, utcnow()),
            )

    def get_sync_batch(self, batch_id, conn=None):
        sql = "SELECT * FROM sync_batches WHERE batch_id = ?"
        if conn is None:
            with self._connect() as connection:
                row = connection.execute(sql, (batch_id,)).fetchone()
        else:
            row = conn.execute(sql, (batch_id,)).fetchone()
        return self._batch_from_row(row) if row else None

    def save_sync_batch(self, batch, conn=None):
        now = utcnow()
        created = batch.get("created_at") or now
        sql = (
            "INSERT OR REPLACE INTO sync_batches"
            "(batch_id, source_id, status, digest, records, result, error, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        params = (
            batch["batch_id"],
            batch["source_id"],
            batch["status"],
            batch["digest"],
            json.dumps(batch["records"], ensure_ascii=False, sort_keys=True),
            json.dumps(batch.get("result"), ensure_ascii=False, sort_keys=True) if batch.get("result") is not None else None,
            batch.get("error"),
            created,
            now,
        )
        if conn is None:
            with self._connect() as connection:
                connection.execute(sql, params)
        else:
            conn.execute(sql, params)
        return self.get_sync_batch(batch["batch_id"], conn=conn)

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True
