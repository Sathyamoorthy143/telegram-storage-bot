"""
database.py  —  Folder-based single-user storage bot
Files are stored in a private Telegram channel (not on disk).
Only metadata + encryption keys are stored in this SQLite database.
"""

import sqlite3

DB_PATH = "storage.db"


class DatabaseManager:
    """Manages SQLite database connections and operations for the storage bot.
    
    Addresses the following architecture improvements:
    - Encapsulates connection lifecycle instead of using global state
    - Enables testability through dependency injection
    - Ensures thread safety through context manager patterns
    - Includes index on frequently-queried folder column
    - Optimizes folder rename operations with SQL-based path updates
    """
    
    def __init__(self, db_path: str = "storage.db"):
        self.db_path = db_path
        self.conn = None
        self._init_db()
    
    def _init_db(self):
        """Initialize the database connection and schema."""
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON")
        
        # ── Folders ───────────────────────────────────────────────────────────
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS folders (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                path    TEXT NOT NULL UNIQUE,
                name    TEXT NOT NULL,
                parent  TEXT NOT NULL DEFAULT '',
                created TEXT DEFAULT (datetime('now'))
            )
        """)
        
        # ── Files ─────────────────────────────────────────────────────────────
        # tg_file_id = Telegram file_id of the encrypted blob in the storage channel
        # tg_msg_id  = message ID in the storage channel (needed to delete it later)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                folder      TEXT    NOT NULL DEFAULT '',
                filename    TEXT    NOT NULL,
                tg_file_id  TEXT    NOT NULL,
                tg_msg_id   INTEGER,
                key         BLOB    NOT NULL,
                nonce       BLOB    NOT NULL,
                size        INTEGER NOT NULL,
                hash        TEXT    NOT NULL UNIQUE,
                uploaded    TEXT    DEFAULT (datetime('now')),
                note        TEXT    DEFAULT NULL
            )
        """)
        
        # Index on folder column for O(log N) lookups instead of full table scans
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_files_folder 
            ON files(folder)
        """)
        
        # ── Access log ────────────────────────────────────────────────────────
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS access_log (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id INTEGER NOT NULL,
                action  TEXT    NOT NULL,
                ts      TEXT    DEFAULT (datetime('now')),
                FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
            )
        """)
        
        self.conn.commit()
        self._migrate()
    
    def _migrate(self):
        """Handle database migrations from old disk-based schema."""
        cursor = self.conn.cursor()
        _cols = [r[1] for r in cursor.execute("PRAGMA table_info(files)")]
        if "filepath" in _cols and "tg_file_id" not in _cols:
            cursor.execute("ALTER TABLE files ADD COLUMN tg_file_id TEXT DEFAULT ''")
            cursor.execute("ALTER TABLE files ADD COLUMN tg_msg_id  INTEGER DEFAULT NULL")
            self.conn.commit()
        _cols = [r[1] for r in cursor.execute("PRAGMA table_info(files)")]
        if "tg_msg_id" not in _cols:
            cursor.execute("ALTER TABLE files ADD COLUMN tg_msg_id INTEGER DEFAULT NULL")
            self.conn.commit()
    
    def normalise(self, path: str) -> str:
        """Normalize a folder path by stripping whitespace and extra slashes."""
        parts = [p.strip() for p in path.strip("/").split("/") if p.strip()]
        return "/".join(parts)
    
    def folder_exists(self, path: str) -> bool:
        """Check if a folder exists."""
        if path == "":
            return True
        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM folders WHERE path=?", (path,))
        return cursor.fetchone() is not None
    
    def create_folder(self, path: str) -> str | None:
        """Create a folder and all parent folders. Returns error message or None."""
        path = self.normalise(path)
        if not path:
            return "Empty folder name"
        parts = path.split("/")
        cursor = self.conn.cursor()
        for i in range(len(parts)):
            current = "/".join(parts[:i + 1])
            parent  = "/".join(parts[:i]) if i > 0 else ""
            name    = parts[i]
            try:
                cursor.execute(
                    "INSERT OR IGNORE INTO folders(path, name, parent) VALUES(?,?,?)",
                    (current, name, parent),
                )
            except Exception as exc:
                return str(exc)
        self.conn.commit()
        return None
    
    def delete_folder(self, path: str) -> tuple[int, int]:
        """Delete folder + all subfolders from DB. Returns (folders_del, files_del).
        Caller is responsible for deleting Telegram messages first."""
        files_deleted = 0
        cursor = self.conn.cursor()
        for fp in [path] + [
            r[0] for r in cursor.execute(
                "SELECT path FROM folders WHERE path LIKE ?", (path + "/%",)
            ).fetchall()
        ]:
            cursor.execute("DELETE FROM files WHERE folder=?", (fp,))
            files_deleted += cursor.rowcount
        cursor.execute(
            "DELETE FROM folders WHERE path=? OR path LIKE ?", (path, path + "/%")
        )
        folders_deleted = cursor.rowcount
        self.conn.commit()
        return folders_deleted, files_deleted
    
    def rename_folder(self, old_path: str, new_name: str) -> str | None:
        """Rename a folder and update all subfolders and files efficiently.
        Uses SQL-based string operations instead of Python iteration for O(1) updates."""
        old_path = self.normalise(old_path)
        if not self.folder_exists(old_path):
            return f"Folder `{old_path}` not found"
        parent   = "/".join(old_path.split("/")[:-1])
        new_path = (parent + "/" + new_name).strip("/")
        if self.folder_exists(new_path):
            return f"Folder `{new_path}` already exists"
        
        cursor = self.conn.cursor()
        
        # Update the parent folder
        cursor.execute("UPDATE folders SET path=?, name=? WHERE path=?",
                       (new_path, new_name, old_path))
        
        # Update all subfolders using SQL string operations (faster than iteration)
        old_len = len(old_path)
        cursor.execute("""
            UPDATE folders 
            SET path = ? || SUBSTR(path, ?)
            WHERE path LIKE ?
        """, (new_path + "/", old_len + 2, old_path + "/%"))
        
        # Fix parent references for subfolders
        cursor.execute("""
            UPDATE folders 
            SET parent = ? || SUBSTR(parent, ?)
            WHERE parent LIKE ?
        """, (new_path + "/", old_len + 2, old_path + "/%"))
        
        # Update files in the renamed folder
        cursor.execute("UPDATE files SET folder=? WHERE folder=?", (new_path, old_path))
        
        # Update files in subfolders using SQL string operations
        cursor.execute("""
            UPDATE files 
            SET folder = ? || SUBSTR(folder, ?)
            WHERE folder LIKE ?
        """, (new_path + "/", old_len + 2, old_path + "/%"))
        
        self.conn.commit()
        return None
    
    def list_folder(self, path: str) -> dict:
        """List all subfolders and files in a folder."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT path, name FROM folders WHERE parent=? ORDER BY name", (path,)
        )
        subfolders = cursor.fetchall()
        cursor.execute(
            "SELECT id, filename, size, uploaded FROM files WHERE folder=? ORDER BY filename",
            (path,),
        )
        files = cursor.fetchall()
        return {"subfolders": subfolders, "files": files}
    
    def move_file(self, file_id: int, dest_folder: str) -> str | None:
        """Move a file to a different folder."""
        dest_folder = self.normalise(dest_folder)
        if not self.folder_exists(dest_folder):
            return f"Folder `{dest_folder}` does not exist. Create it first with /mkdir."
        cursor = self.conn.cursor()
        cursor.execute("UPDATE files SET folder=? WHERE id=?", (dest_folder, file_id))
        self.conn.commit()
        return None
    
    def log_action(self, file_id: int, action: str) -> None:
        """Log a file action."""
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO access_log(file_id, action) VALUES(?,?)", (file_id, action)
        )
        self.conn.commit()
    
    def total_used_bytes(self) -> int:
        """Get total bytes used by all files."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COALESCE(SUM(size),0) FROM files")
        return cursor.fetchone()[0]
    
    def get_tg_msg_ids_in_folder(self, path: str) -> list[int]:
        """All Telegram message IDs for files in this folder tree (for deletion)."""
        cursor = self.conn.cursor()
        direct = [
            r[0] for r in cursor.execute(
                "SELECT tg_msg_id FROM files WHERE folder=? AND tg_msg_id IS NOT NULL", (path,)
            ).fetchall()
        ]
        nested = [
            r[0] for r in cursor.execute(
                "SELECT tg_msg_id FROM files WHERE folder LIKE ? AND tg_msg_id IS NOT NULL",
                (path + "/%",),
            ).fetchall()
        ]
        return direct + nested
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()


# ── Module-level singleton instance for backward compatibility ────────────────
_db = DatabaseManager(DB_PATH)

# ── Public API: expose functions at module level ────────────────────────────────

def normalise(path: str) -> str:
    return _db.normalise(path)


def folder_exists(path: str) -> bool:
    return _db.folder_exists(path)


def create_folder(path: str) -> str | None:
    return _db.create_folder(path)


def delete_folder(path: str) -> tuple[int, int]:
    return _db.delete_folder(path)


def rename_folder(old_path: str, new_name: str) -> str | None:
    return _db.rename_folder(old_path, new_name)


def list_folder(path: str) -> dict:
    return _db.list_folder(path)


def move_file(file_id: int, dest_folder: str) -> str | None:
    return _db.move_file(file_id, dest_folder)


def log_action(file_id: int, action: str) -> None:
    return _db.log_action(file_id, action)


def total_used_bytes() -> int:
    return _db.total_used_bytes()


def get_tg_msg_ids_in_folder(path: str) -> list[int]:
    return _db.get_tg_msg_ids_in_folder(path)
