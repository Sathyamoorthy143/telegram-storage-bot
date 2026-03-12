"""
database.py  —  Folder-based single-user storage bot
Stores folders as paths (e.g. "Personal/ID Docs") and files inside them.
"""

import sqlite3

DB_PATH = "storage.db"

conn   = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor()
cursor.execute("PRAGMA foreign_keys = ON")

# ── Folders ───────────────────────────────────────────────────────────────────
# path is the full folder path, e.g.  "Personal"  or  "Personal/ID Docs"
cursor.execute("""
    CREATE TABLE IF NOT EXISTS folders (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        path       TEXT    NOT NULL UNIQUE,       -- full path  e.g. Personal/ID Docs
        name       TEXT    NOT NULL,              -- leaf name  e.g. ID Docs
        parent     TEXT    NOT NULL DEFAULT '',   -- parent path, '' = root
        created    TEXT    DEFAULT (datetime('now'))
    )
""")

# ── Files ─────────────────────────────────────────────────────────────────────
cursor.execute("""
    CREATE TABLE IF NOT EXISTS files (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        folder     TEXT    NOT NULL DEFAULT '',   -- folder path, '' = root
        filename   TEXT    NOT NULL,
        filepath   TEXT    NOT NULL,
        key        BLOB    NOT NULL,
        nonce      BLOB    NOT NULL,
        size       INTEGER NOT NULL,
        hash       TEXT    NOT NULL UNIQUE,
        uploaded   TEXT    DEFAULT (datetime('now')),
        note       TEXT    DEFAULT NULL
    )
""")

# ── Access log ────────────────────────────────────────────────────────────────
cursor.execute("""
    CREATE TABLE IF NOT EXISTS access_log (
        id       INTEGER PRIMARY KEY AUTOINCREMENT,
        file_id  INTEGER NOT NULL,
        action   TEXT    NOT NULL,
        ts       TEXT    DEFAULT (datetime('now')),
        FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
    )
""")

conn.commit()


# ═══════════════════════════════════════════════════════════════════════════════
#  Folder helpers
# ═══════════════════════════════════════════════════════════════════════════════

def normalise(path: str) -> str:
    """Strip slashes, collapse spaces, e.g.  '/Personal / ID Docs/' → 'Personal/ID Docs'"""
    parts = [p.strip() for p in path.strip("/").split("/") if p.strip()]
    return "/".join(parts)


def folder_exists(path: str) -> bool:
    if path == "":
        return True                     # root always exists
    cursor.execute("SELECT id FROM folders WHERE path=?", (path,))
    return cursor.fetchone() is not None


def create_folder(path: str) -> str | None:
    """
    Create a folder (and all missing parents).
    Returns error string on failure, None on success.
    """
    path = normalise(path)
    if not path:
        return "Empty folder name"

    parts = path.split("/")
    for i in range(len(parts)):
        current = "/".join(parts[:i+1])
        parent  = "/".join(parts[:i]) if i > 0 else ""
        name    = parts[i]
        try:
            cursor.execute(
                "INSERT OR IGNORE INTO folders(path, name, parent) VALUES(?,?,?)",
                (current, name, parent),
            )
        except Exception as exc:
            return str(exc)

    conn.commit()
    return None


def delete_folder(path: str) -> tuple[int, int]:
    """
    Delete folder and all sub-folders + their files.
    Returns (folders_deleted, files_deleted).
    """
    import os
    # Find all folders under this path (inclusive)
    cursor.execute(
        "SELECT path FROM folders WHERE path=? OR path LIKE ?",
        (path, path + "/%"),
    )
    sub_paths = [r[0] for r in cursor.fetchall()]

    files_deleted = 0
    for fp in sub_paths:
        cursor.execute("SELECT filepath FROM files WHERE folder=?", (fp,))
        for (disk_path,) in cursor.fetchall():
            if os.path.exists(disk_path):
                os.remove(disk_path)
        cursor.execute("DELETE FROM files WHERE folder=?", (fp,))
        files_deleted += cursor.rowcount

    # Also delete files directly in this folder
    cursor.execute("SELECT filepath FROM files WHERE folder=?", (path,))
    for (disk_path,) in cursor.fetchall():
        if os.path.exists(disk_path):
            os.remove(disk_path)
    cursor.execute("DELETE FROM files WHERE folder=?", (path,))
    files_deleted += cursor.rowcount

    cursor.execute(
        "DELETE FROM folders WHERE path=? OR path LIKE ?",
        (path, path + "/%"),
    )
    folders_deleted = cursor.rowcount
    conn.commit()
    return folders_deleted, files_deleted


def rename_folder(old_path: str, new_name: str) -> str | None:
    """Rename a folder leaf. Returns error string or None."""
    old_path = normalise(old_path)
    if not folder_exists(old_path):
        return f"Folder `{old_path}` not found"

    parent    = "/".join(old_path.split("/")[:-1])
    new_path  = (parent + "/" + new_name).strip("/")

    if folder_exists(new_path):
        return f"Folder `{new_path}` already exists"

    # Rename the folder itself
    cursor.execute(
        "UPDATE folders SET path=?, name=? WHERE path=?",
        (new_path, new_name, old_path),
    )
    # Rename all sub-paths
    cursor.execute(
        "SELECT path FROM folders WHERE path LIKE ?", (old_path + "/%",)
    )
    for (sub,) in cursor.fetchall():
        new_sub = new_path + sub[len(old_path):]
        new_sub_name = new_sub.split("/")[-1]
        new_sub_parent = "/".join(new_sub.split("/")[:-1])
        cursor.execute(
            "UPDATE folders SET path=?, name=?, parent=? WHERE path=?",
            (new_sub, new_sub_name, new_sub_parent, sub),
        )
    # Update files
    cursor.execute(
        "UPDATE files SET folder=? WHERE folder=?", (new_path, old_path)
    )
    cursor.execute(
        "SELECT id FROM files WHERE folder LIKE ?", (old_path + "/%",)
    )
    cursor.execute(
        "UPDATE files SET folder = REPLACE(folder, ?, ?) WHERE folder LIKE ?",
        (old_path + "/", new_path + "/", old_path + "/%"),
    )
    conn.commit()
    return None


def list_folder(path: str) -> dict:
    """
    Return dict with keys:
      'subfolders' : list of (path, name)
      'files'      : list of (id, filename, size, uploaded)
    """
    # Direct children folders only
    cursor.execute(
        "SELECT path, name FROM folders WHERE parent=? ORDER BY name",
        (path,),
    )
    subfolders = cursor.fetchall()

    cursor.execute(
        "SELECT id, filename, size, uploaded FROM files WHERE folder=? ORDER BY filename",
        (path,),
    )
    files = cursor.fetchall()

    return {"subfolders": subfolders, "files": files}


def move_file(file_id: int, dest_folder: str) -> str | None:
    """Move a file to a different folder. Returns error or None."""
    dest_folder = normalise(dest_folder)
    if not folder_exists(dest_folder):
        return f"Folder `{dest_folder}` does not exist. Create it first with /mkdir."
    cursor.execute("UPDATE files SET folder=? WHERE id=?", (dest_folder, file_id))
    conn.commit()
    return None


def log_action(file_id: int, action: str) -> None:
    cursor.execute(
        "INSERT INTO access_log(file_id, action) VALUES(?,?)", (file_id, action)
    )
    conn.commit()


def total_used_bytes() -> int:
    cursor.execute("SELECT COALESCE(SUM(size),0) FROM files")
    return cursor.fetchone()[0]
