"""
📁 Personal File Storage Bot  —  Folder Edition
════════════════════════════════════════════════════════════════════════════════

CONCEPT
  The bot works exactly like a file manager on your phone/PC.
  You create folders, navigate into them, and upload files there.
  Files are always encrypted with AES-256 before being stored on disk.

QUICK START
  1. Set  BOT_TOKEN  environment variable.
  2. Set  OWNER_ID   to your Telegram numeric user ID.
  3. pip install "python-telegram-bot[job-queue]" cryptography
  4. python bot.py
  5. Send /start to the bot.

FOLDER STRUCTURE EXAMPLE
  📁 root
  ├── 📁 Personal
  │   ├── 📁 ID Documents
  │   │   ├── passport.pdf
  │   │   └── aadhar_card.jpg
  │   ├── 📁 Photos
  │   └── resume.pdf
  ├── 📁 Education
  │   ├── 📁 Certificates
  │   ├── 📁 Marksheets
  │   └── notes.txt
  ├── 📁 Medical
  │   ├── 📁 2024
  │   └── insurance_card.pdf
  ├── 📁 Financial
  │   ├── 📁 Tax
  │   └── bank_statement.pdf
  └── 📁 Future Plans
      └── goals_2026.docx

NAVIGATION
  You always have a "current folder" (like a terminal's working directory).
  /cd <folder>  moves into a folder.
  /cd ..        goes up one level.
  /cd /         returns to root.
  /ls           shows what's in your current folder.
  /ls <folder>  shows any folder without changing your location.

UPLOADING
  Just send any file — it lands in your current folder automatically.
  No extra commands needed.

════════════════════════════════════════════════════════════════════════════════
"""

import asyncio
import csv
import datetime
import hashlib
import io
import logging
import os
import zipfile
import tempfile
import shutil
import sqlite3
from functools import wraps

from telegram import Update, BotCommand
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

import database as db
from crypto import encrypt_file, decrypt_file


# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s  %(levelname)s  %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ── Config ────────────────────────────────────────────────────────────────────

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable is not set")

OWNER_ID          = int(os.getenv("OWNER_ID", "8250992325"))   # ← your Telegram user ID
STORAGE           = "storage"
os.makedirs(STORAGE, exist_ok=True)

MAX_FILE_SIZE     = 20 * 1024 * 1024                                           # 20 MB
QUOTA_BYTES       = int(os.getenv("QUOTA_MB", "50000")) * 1024 * 1024           # 500 MB default
IMAGE_EXTENSIONS  = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}
MIME_TO_EXT       = {
    "image/jpeg": ".jpg", "image/png": ".png", "image/gif": ".gif",
    "image/webp": ".webp", "video/mp4": ".mp4", "audio/mpeg": ".mp3",
    "audio/ogg": ".ogg",
}


# ── Auth ──────────────────────────────────────────────────────────────────────

def auth(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != OWNER_ID:
            await update.message.reply_text("⛔ Private bot.")
            return
        return await func(update, context)
    return wrapper


# ── Current-folder state ──────────────────────────────────────────────────────

def _cwd(context: ContextTypes.DEFAULT_TYPE) -> str:
    """Return current working folder path ('' = root)."""
    return context.user_data.get("cwd", "")


def _set_cwd(context: ContextTypes.DEFAULT_TYPE, path: str) -> None:
    context.user_data["cwd"] = path


def _display_path(path: str) -> str:
    """Show path as  📁 root  or  📁 Personal/ID Docs"""
    return f"📁 {path}" if path else "📁 root"


# ── Utilities ─────────────────────────────────────────────────────────────────

def _fmt(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def _dated(base: str, ext: str) -> str:
    return f"{base}_{datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S')}{ext}"


def _resolve(context: ContextTypes.DEFAULT_TYPE, user_input: str) -> str:
    """
    Resolve a user-typed path relative to cwd.
      /Personal/ID Docs  → absolute  Personal/ID Docs
      ID Docs            → relative to cwd
      ..                 → parent folder
    """
    raw = user_input.strip()

    if raw in ("/", ""):
        return ""

    if raw.startswith("/"):
        # Absolute path
        return db.normalise(raw)

    if raw == "..":
        cwd = _cwd(context)
        if "/" in cwd:
            return cwd.rsplit("/", 1)[0]
        return ""

    # Relative — append to cwd
    cwd = _cwd(context)
    return db.normalise((cwd + "/" + raw) if cwd else raw)


def _build_tree(path: str, prefix: str = "", depth: int = 0, max_depth: int = 5) -> str:
    """Recursively build an ASCII tree of folders and files."""
    if depth > max_depth:
        return ""

    result = db.list_folder(path)
    lines  = []

    items = [("folder", sf[0], sf[1]) for sf in result["subfolders"]] + \
            [("file",   str(f[0]), f[1]) for f in result["files"]]

    for i, (kind, fid_or_path, name) in enumerate(items):
        is_last   = (i == len(items) - 1)
        connector = "└── " if is_last else "├── "
        extender  = "    " if is_last else "│   "

        if kind == "folder":
            lines.append(f"{prefix}{connector}📁 {name}")
            lines.append(_build_tree(fid_or_path, prefix + extender, depth + 1, max_depth))
        else:
            # fid_or_path is the file id string here
            db.cursor.execute("SELECT size FROM files WHERE id=?", (fid_or_path,))
            row = db.cursor.fetchone()
            size_str = f"  ({_fmt(row[0])})" if row else ""
            lines.append(f"{prefix}{connector}📄 {name}{size_str}")

    return "\n".join(l for l in lines if l)


# ── /start ────────────────────────────────────────────────────────────────────

@auth
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cwd = _cwd(context)
    await update.message.reply_text(
        "📁 *Personal File Storage Bot*\n\n"
        f"📍 Current folder: {_display_path(cwd)}\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "📂 *NAVIGATION*\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "`/ls` — list current folder\n"
        "`/ls <folder>` — list any folder\n"
        "`/cd <folder>` — enter a folder\n"
        "`/cd ..` — go up one level\n"
        "`/cd /` — go to root\n"
        "`/tree` — full folder tree\n"
        "`/pwd` — show current location\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "📁 *FOLDERS*\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "`/mkdir <name>` — create folder\n"
        "`/mkdir Personal/ID Docs` — nested create\n"
        "`/rmdir <folder>` — delete folder + contents\n"
        "`/mvdir <folder> <new name>` — rename folder\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "📄 *FILES*\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "Send any file → saved to current folder\n"
        "`/get <id>` — download a file\n"
        "`/info <id>` — file details\n"
        "`/note <id> <text>` — add note to file\n"
        "`/mv <id> <folder>` — move file to folder\n"
        "`/rename <id> <new name>` — rename file\n"
        "`/rm <id>` — delete file\n"
        "`/find <name>` — search all files\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "📊 *MISC*\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "`/stats` — storage overview\n"
        "`/recent` — last 10 uploads\n"
        "`/backup` — download encrypted backup\n"
        "`/restore` — restore from backup zip\n"
        "`/export` — export file list as CSV\n",
        parse_mode="Markdown",
    )


# ── /pwd ──────────────────────────────────────────────────────────────────────

@auth
async def pwd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cwd = _cwd(context)
    await update.message.reply_text(
        f"📍 You are in: *{_display_path(cwd)}*", parse_mode="Markdown"
    )


# ── /cd ───────────────────────────────────────────────────────────────────────

@auth
async def cd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "Usage:\n`/cd <folder name>` — enter folder\n"
            "`/cd ..` — go up\n`/cd /` — go to root",
            parse_mode="Markdown",
        )
        return

    target = " ".join(context.args)
    path   = _resolve(context, target)

    if not db.folder_exists(path):
        await update.message.reply_text(
            f"❌ Folder `{path or 'root'}` does not exist.\n"
            "Use /ls to see what's here, or /mkdir to create it.",
            parse_mode="Markdown",
        )
        return

    _set_cwd(context, path)
    content = db.list_folder(path)
    nsub    = len(content["subfolders"])
    nfiles  = len(content["files"])

    await update.message.reply_text(
        f"📂 Entered: *{_display_path(path)}*\n"
        f"Contains: {nsub} folder(s),  {nfiles} file(s)\n\n"
        "Use /ls to see contents.",
        parse_mode="Markdown",
    )


# ── /ls ───────────────────────────────────────────────────────────────────────

@auth
async def ls(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.args:
        path = _resolve(context, " ".join(context.args))
    else:
        path = _cwd(context)

    if not db.folder_exists(path):
        await update.message.reply_text(
            f"❌ Folder `{path}` not found.", parse_mode="Markdown"
        )
        return

    content    = db.list_folder(path)
    subfolders = content["subfolders"]
    files      = content["files"]
    header     = f"📂 *{_display_path(path)}*\n"

    if not subfolders and not files:
        await update.message.reply_text(
            header + "\n_Empty folder._\n\n"
            "Send a file to upload here, or /mkdir to create a subfolder.",
            parse_mode="Markdown",
        )
        return

    lines = [header]

    if subfolders:
        lines.append("*Folders:*")
        for fpath, fname in subfolders:
            sub    = db.list_folder(fpath)
            nsub   = len(sub["subfolders"])
            nfiles = len(sub["files"])
            lines.append(f"  📁 *{fname}*  ›  {nsub} folder(s), {nfiles} file(s)")

    if files:
        if subfolders:
            lines.append("")
        lines.append("*Files:*")
        for fid, name, size, uploaded in files:
            lines.append(f"  `{fid}` 📄 {name}  ({_fmt(size)})  _{uploaded or '—'}_")

    msg = "\n".join(lines)
    if len(msg) > 4000:
        # split into chunks
        chunks, chunk = [], ""
        for line in lines:
            if len(chunk) + len(line) + 1 > 4000:
                await update.message.reply_text(chunk, parse_mode="Markdown")
                chunk = ""
            chunk += line + "\n"
        if chunk:
            await update.message.reply_text(chunk, parse_mode="Markdown")
    else:
        await update.message.reply_text(msg, parse_mode="Markdown")


# ── /tree ─────────────────────────────────────────────────────────────────────

@auth
async def tree(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.args:
        path = _resolve(context, " ".join(context.args))
    else:
        path = ""          # always show full tree from root

    if not db.folder_exists(path):
        await update.message.reply_text(
            f"❌ Folder `{path}` not found.", parse_mode="Markdown"
        )
        return

    label = _display_path(path)
    body  = _build_tree(path)

    msg = f"🌳 *Tree: {label}*\n\n"
    if body:
        msg += f"```\n{body}\n```"
    else:
        msg += "_Empty._"

    if len(msg) > 4000:
        msg = msg[:3950] + "\n…(truncated, use /ls to browse)"

    await update.message.reply_text(msg, parse_mode="Markdown")


# ── /mkdir ────────────────────────────────────────────────────────────────────

@auth
async def mkdir(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "Usage: `/mkdir <name>`  or  `/mkdir Personal/ID Docs`\n"
            "Nested folders are created automatically.",
            parse_mode="Markdown",
        )
        return

    raw  = " ".join(context.args)
    # If not absolute, resolve relative to cwd
    path = _resolve(context, raw) if not raw.startswith("/") else db.normalise(raw)
    # If user typed a plain name (no slash), nest under cwd
    if "/" not in raw and not raw.startswith("/"):
        cwd  = _cwd(context)
        path = db.normalise((cwd + "/" + raw) if cwd else raw)

    if db.folder_exists(path):
        await update.message.reply_text(
            f"ℹ️ Folder *{path}* already exists.", parse_mode="Markdown"
        )
        return

    err = db.create_folder(path)
    if err:
        await update.message.reply_text(f"❌ {err}")
        return

    await update.message.reply_text(
        f"✅ Folder *{path}* created!\n\n"
        f"Use `/cd {path.split('/')[-1]}` to enter it.",
        parse_mode="Markdown",
    )


# ── /rmdir ────────────────────────────────────────────────────────────────────

@auth
async def rmdir(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/rmdir <folder>`", parse_mode="Markdown")
        return

    path = _resolve(context, " ".join(context.args))

    if not db.folder_exists(path) or path == "":
        await update.message.reply_text(
            f"❌ Folder `{path or 'root'}` not found or cannot delete root.",
            parse_mode="Markdown",
        )
        return

    # Count what will be deleted for confirmation message
    content = db.list_folder(path)
    nf, nd  = len(content["files"]), len(content["subfolders"])

    folders_del, files_del = db.delete_folder(path)

    # If we deleted the current folder, go up
    cwd = _cwd(context)
    if cwd == path or cwd.startswith(path + "/"):
        parent = path.rsplit("/", 1)[0] if "/" in path else ""
        _set_cwd(context, parent)

    await update.message.reply_text(
        f"🗑️ Deleted folder *{path}*\n"
        f"Removed {folders_del} folder(s) and {files_del} file(s).",
        parse_mode="Markdown",
    )


# ── /mvdir ────────────────────────────────────────────────────────────────────

@auth
async def mvdir(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text(
            "Usage: `/mvdir <folder> <new name>`\n"
            "Example: `/mvdir Medical 2024 Medical Records`",
            parse_mode="Markdown",
        )
        return

    old_path = _resolve(context, context.args[0])
    new_name = " ".join(context.args[1:])

    err = db.rename_folder(old_path, new_name)
    if err:
        await update.message.reply_text(f"❌ {err}", parse_mode="Markdown")
        return

    parent   = "/".join(old_path.split("/")[:-1])
    new_path = (parent + "/" + new_name).strip("/")

    # Update cwd if we renamed the current folder
    cwd = _cwd(context)
    if cwd == old_path:
        _set_cwd(context, new_path)

    await update.message.reply_text(
        f"✏️ Renamed *{old_path}* → *{new_path}*", parse_mode="Markdown"
    )


# ── Core upload ───────────────────────────────────────────────────────────────

async def _do_upload(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    tg_file_id: str,
    filename: str,
    file_size: int | None,
) -> None:
    cwd = _cwd(context)

    if file_size and file_size > MAX_FILE_SIZE:
        await update.message.reply_text("❌ File too large (max 20 MB)")
        return

    used = db.total_used_bytes()
    if file_size and QUOTA_BYTES and (used + file_size) > QUOTA_BYTES:
        await update.message.reply_text(
            f"❌ Storage quota full ({_fmt(used)} / {_fmt(QUOTA_BYTES)}).\n"
            "Delete some files first with /rm."
        )
        return

    folder_display = _display_path(cwd)
    await update.message.reply_text(
        f"⏳ Uploading *{filename}* → {folder_display}…",
        parse_mode="Markdown",
    )

    try:
        tg = await context.bot.get_file(tg_file_id)
        data = bytes(await tg.download_as_bytearray())
    except Exception as exc:
        logger.error("Download error: %s", exc)
        await update.message.reply_text("❌ Failed to download from Telegram")
        return

    sha = hashlib.sha256(data).hexdigest()
    db.cursor.execute("SELECT id, folder FROM files WHERE hash=?", (sha,))
    existing = db.cursor.fetchone()
    if existing:
        loc = _display_path(existing[1])
        await update.message.reply_text(
            f"⚠️ Duplicate — identical file already stored as ID `{existing[0]}` in {loc}",
            parse_mode="Markdown",
        )
        return

    try:
        encrypted, nonce, key = encrypt_file(data)
        filepath = os.path.join(STORAGE, f"{tg_file_id}.enc")
        with open(filepath, "wb") as f:
            f.write(encrypted)
    except Exception as exc:
        logger.error("Encrypt error: %s", exc)
        await update.message.reply_text("❌ Encryption failed")
        return

    try:
        db.cursor.execute(
            "INSERT INTO files(folder, filename, filepath, key, nonce, size, hash, uploaded) "
            "VALUES(?,?,?,?,?,?,?,datetime('now'))",
            (cwd, filename, filepath, key, nonce, len(data), sha),
        )
        new_id = db.cursor.lastrowid
        db.conn.commit()
    except Exception as exc:
        logger.error("DB error: %s", exc)
        await update.message.reply_text("❌ Database error")
        return

    db.log_action(new_id, "upload")

    await update.message.reply_text(
        f"✅ *Saved!*\n"
        f"📄 {filename}\n"
        f"🆔 ID: `{new_id}`\n"
        f"📁 Folder: {folder_display}\n"
        f"📦 Size: {_fmt(len(data))}",
        parse_mode="Markdown",
    )


# ── Upload handlers (all file types) ─────────────────────────────────────────

@auth
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("awaiting_restore"):
        await _do_restore(update, context)
        return
    doc = update.message.document
    await _do_upload(update, context, doc.file_id, doc.file_name or "file", doc.file_size)


@auth
async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    photo   = update.message.photo[-1]
    caption = (update.message.caption or "").strip()
    name    = _dated(caption or "photo", ".jpg")
    await _do_upload(update, context, photo.file_id, name, photo.file_size)


@auth
async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    video = update.message.video
    ext   = MIME_TO_EXT.get(video.mime_type or "", ".mp4")
    name  = video.file_name or _dated("video", ext)
    await _do_upload(update, context, video.file_id, name, video.file_size)


@auth
async def handle_audio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    audio = update.message.audio
    ext   = MIME_TO_EXT.get(audio.mime_type or "", ".mp3")
    name  = audio.file_name or _dated("audio", ext)
    await _do_upload(update, context, audio.file_id, name, audio.file_size)


@auth
async def handle_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    voice = update.message.voice
    await _do_upload(update, context, voice.file_id, _dated("voice", ".ogg"), voice.file_size)


# ── /get ─────────────────────────────────────────────────────────────────────

@auth
async def get_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: `/get <id>`", parse_mode="Markdown")
        return

    file_id = int(context.args[0])
    db.cursor.execute(
        "SELECT filepath, key, nonce, filename FROM files WHERE id=?", (file_id,)
    )
    row = db.cursor.fetchone()
    if not row:
        await update.message.reply_text(f"❌ No file with ID `{file_id}`", parse_mode="Markdown")
        return

    path, key, nonce, filename = row

    if not os.path.exists(path):
        await update.message.reply_text("❌ File missing from disk.")
        return

    await update.message.reply_text(
        f"⏳ Decrypting *{filename}*…", parse_mode="Markdown"
    )

    try:
        with open(path, "rb") as f:
            enc = f.read()
        data = decrypt_file(enc, nonce, key)
    except Exception as exc:
        logger.error("Decrypt error: %s", exc)
        await update.message.reply_text("❌ Decryption failed")
        return

    # Image preview
    ext = os.path.splitext(filename)[1].lower()
    if ext in IMAGE_EXTENSIONS:
        try:
            await update.message.reply_photo(
                io.BytesIO(data),
                caption=f"📸 *{filename}*",
                parse_mode="Markdown",
            )
        except Exception:
            pass

    stream      = io.BytesIO(data)
    stream.name = filename
    await update.message.reply_document(stream, filename=filename)
    db.log_action(file_id, "download")


# ── /info ─────────────────────────────────────────────────────────────────────

@auth
async def info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: `/info <id>`", parse_mode="Markdown")
        return

    file_id = int(context.args[0])
    db.cursor.execute(
        "SELECT filename, folder, size, hash, uploaded, note FROM files WHERE id=?",
        (file_id,),
    )
    row = db.cursor.fetchone()
    if not row:
        await update.message.reply_text(f"❌ No file with ID `{file_id}`", parse_mode="Markdown")
        return

    name, folder, size, sha, uploaded, note = row

    db.cursor.execute(
        "SELECT action, ts FROM access_log WHERE file_id=? ORDER BY ts DESC LIMIT 5",
        (file_id,),
    )
    logs = db.cursor.fetchall()
    log_lines = "\n".join(
        f"  {'⬆️' if a=='upload' else '⬇️'} `{a}` — {t}"
        for a, t in logs
    ) or "  _none_"

    await update.message.reply_text(
        f"📄 *{name}*\n"
        f"🆔 ID: `{file_id}`\n"
        f"📁 Folder: {_display_path(folder)}\n"
        f"📦 Size: {_fmt(size)}\n"
        f"🕐 Uploaded: {uploaded or '—'}\n"
        f"📝 Note: {note or '_none_'}\n"
        f"🔑 SHA-256:\n`{sha}`\n\n"
        f"📋 *Recent access:*\n{log_lines}",
        parse_mode="Markdown",
    )


# ── /note ─────────────────────────────────────────────────────────────────────

@auth
async def note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2 or not context.args[0].isdigit():
        await update.message.reply_text(
            "Usage: `/note <id> <text>`", parse_mode="Markdown"
        )
        return
    file_id = int(context.args[0])
    text    = " ".join(context.args[1:])
    db.cursor.execute("UPDATE files SET note=? WHERE id=?", (text, file_id))
    db.conn.commit()
    if db.cursor.rowcount == 0:
        await update.message.reply_text(f"❌ No file with ID `{file_id}`", parse_mode="Markdown")
        return
    await update.message.reply_text(f"📝 Note saved for `{file_id}`", parse_mode="Markdown")


# ── /mv (move file to another folder) ────────────────────────────────────────

@auth
async def mv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2 or not context.args[0].isdigit():
        await update.message.reply_text(
            "Usage: `/mv <id> <folder>`\n"
            "Example: `/mv 5 Personal/ID Docs`",
            parse_mode="Markdown",
        )
        return

    file_id = int(context.args[0])
    dest    = _resolve(context, " ".join(context.args[1:]))

    db.cursor.execute("SELECT filename FROM files WHERE id=?", (file_id,))
    row = db.cursor.fetchone()
    if not row:
        await update.message.reply_text(f"❌ No file with ID `{file_id}`", parse_mode="Markdown")
        return

    err = db.move_file(file_id, dest)
    if err:
        await update.message.reply_text(f"❌ {err}", parse_mode="Markdown")
        return

    await update.message.reply_text(
        f"📦 *{row[0]}* moved to {_display_path(dest)}",
        parse_mode="Markdown",
    )


# ── /rename (rename file) ─────────────────────────────────────────────────────

@auth
async def rename_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2 or not context.args[0].isdigit():
        await update.message.reply_text(
            "Usage: `/rename <id> <new name>`", parse_mode="Markdown"
        )
        return

    file_id  = int(context.args[0])
    new_name = " ".join(context.args[1:])

    db.cursor.execute("SELECT filename FROM files WHERE id=?", (file_id,))
    row = db.cursor.fetchone()
    if not row:
        await update.message.reply_text(f"❌ No file with ID `{file_id}`", parse_mode="Markdown")
        return

    db.cursor.execute("UPDATE files SET filename=? WHERE id=?", (new_name, file_id))
    db.conn.commit()
    await update.message.reply_text(
        f"✏️ Renamed *{row[0]}* → *{new_name}*", parse_mode="Markdown"
    )


# ── /rm (delete file) ─────────────────────────────────────────────────────────

@auth
async def rm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "Usage: `/rm <id>` or `/rm <id1> <id2> <id3>`", parse_mode="Markdown"
        )
        return

    deleted, errors = [], []
    for arg in context.args:
        if not arg.isdigit():
            errors.append(f"`{arg}` is not a valid ID")
            continue
        fid = int(arg)
        db.cursor.execute("SELECT filepath, filename FROM files WHERE id=?", (fid,))
        row = db.cursor.fetchone()
        if not row:
            errors.append(f"ID `{fid}` not found")
            continue
        path, filename = row
        if os.path.exists(path):
            os.remove(path)
        db.cursor.execute("DELETE FROM files WHERE id=?", (fid,))
        db.conn.commit()
        deleted.append(f"`{fid}` {filename}")

    msg = ""
    if deleted:
        msg += "🗑️ *Deleted:*\n" + "\n".join(deleted)
    if errors:
        msg += ("\n\n" if msg else "") + "❌ *Errors:*\n" + "\n".join(errors)
    await update.message.reply_text(msg or "Nothing deleted.", parse_mode="Markdown")


# ── /find ─────────────────────────────────────────────────────────────────────

@auth
async def find(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/find <name>`", parse_mode="Markdown")
        return

    query = " ".join(context.args)
    db.cursor.execute(
        "SELECT id, filename, folder, size, uploaded FROM files "
        "WHERE filename LIKE ? ORDER BY folder, filename",
        (f"%{query}%",),
    )
    rows = db.cursor.fetchall()

    if not rows:
        await update.message.reply_text(
            f"🔍 No files matching *{query}*", parse_mode="Markdown"
        )
        return

    msg = f"🔍 *Results for \"{query}\" ({len(rows)}):*\n\n"
    for fid, name, folder, size, uploaded in rows:
        msg += (
            f"`{fid}` 📄 {name}  ({_fmt(size)})\n"
            f"       📁 {_display_path(folder)}\n"
        )
    if len(msg) > 4000:
        msg = msg[:3950] + "\n…(truncated)"
    await update.message.reply_text(msg, parse_mode="Markdown")


# ── /stats ────────────────────────────────────────────────────────────────────

@auth
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db.cursor.execute("SELECT COUNT(*), COALESCE(SUM(size),0) FROM files")
    total_files, total_size = db.cursor.fetchone()

    db.cursor.execute("SELECT COUNT(*) FROM folders")
    total_folders = db.cursor.fetchone()[0]

    db.cursor.execute("SELECT filename, size FROM files ORDER BY size DESC LIMIT 1")
    largest = db.cursor.fetchone()

    db.cursor.execute(
        "SELECT filename, uploaded FROM files ORDER BY uploaded DESC LIMIT 1"
    )
    newest = db.cursor.fetchone()

    quota_pct = (total_size / QUOTA_BYTES * 100) if QUOTA_BYTES else 0
    bar_len   = 20
    filled    = int(bar_len * min(quota_pct, 100) / 100)
    bar       = "█" * filled + "░" * (bar_len - filled)

    # Per-folder breakdown
    db.cursor.execute(
        "SELECT folder, COUNT(*), SUM(size) FROM files GROUP BY folder ORDER BY folder"
    )
    folder_rows = db.cursor.fetchall()

    msg = (
        f"📊 *Storage Stats*\n\n"
        f"📁 Folders: {total_folders}\n"
        f"📄 Files: {total_files}\n"
        f"💾 Used: {_fmt(total_size)} / {_fmt(QUOTA_BYTES)}\n"
        f"[{bar}] {quota_pct:.1f}%\n"
    )

    if largest:
        msg += f"\n📦 Largest: *{largest[0]}*  ({_fmt(largest[1])})"
    if newest:
        msg += f"\n🕐 Newest: *{newest[0]}*  ({newest[1] or '—'})"

    if folder_rows:
        msg += "\n\n*By folder:*\n"
        for folder, cnt, fsize in folder_rows:
            msg += f"  {_display_path(folder)}: {cnt} file(s), {_fmt(fsize)}\n"

    await update.message.reply_text(msg, parse_mode="Markdown")


# ── /recent ───────────────────────────────────────────────────────────────────

@auth
async def recent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db.cursor.execute(
        "SELECT id, filename, folder, size, uploaded "
        "FROM files ORDER BY uploaded DESC LIMIT 10"
    )
    rows = db.cursor.fetchall()

    if not rows:
        await update.message.reply_text("📭 No files yet.")
        return

    msg = "🕐 *Last 10 uploads:*\n\n"
    for fid, name, folder, size, uploaded in rows:
        msg += (
            f"`{fid}` 📄 *{name}*  ({_fmt(size)})\n"
            f"       📁 {_display_path(folder)}  ·  _{uploaded or '—'}_\n"
        )
    await update.message.reply_text(msg, parse_mode="Markdown")


# ── /dupes ────────────────────────────────────────────────────────────────────

@auth
async def dupes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Find duplicate files in two ways:
      1. Same CONTENT (identical hash) — should never happen due to upload guard,
         but catches any manual DB entries or restored files.
      2. Same FILENAME across different folders — different content, same name.
    """

    # ── 1. Content duplicates (identical hash) ────────────────────────────────
    db.cursor.execute("""
        SELECT hash, COUNT(*) as cnt
        FROM files
        GROUP BY hash
        HAVING cnt > 1
        ORDER BY cnt DESC
    """)
    content_dups = db.cursor.fetchall()

    # ── 2. Filename duplicates (same name, different location/content) ─────────
    db.cursor.execute("""
        SELECT filename, COUNT(*) as cnt
        FROM files
        GROUP BY filename
        HAVING cnt > 1
        ORDER BY cnt DESC, filename
    """)
    name_dups = db.cursor.fetchall()

    if not content_dups and not name_dups:
        await update.message.reply_text(
            "✅ *No duplicates found!*\n\n"
            "Every file has unique content and a unique filename.",
            parse_mode="Markdown",
        )
        return

    msg = "🔍 *Duplicate File Report*\n"

    # ── Content duplicates section ────────────────────────────────────────────
    if content_dups:
        msg += f"\n━━━━━━━━━━━━━━━━━━\n"
        msg += f"♻️ *Identical content ({len(content_dups)} group(s)):*\n"
        msg += "_These files are 100% the same — safe to delete extras._\n\n"

        for sha, cnt in content_dups:
            db.cursor.execute(
                "SELECT id, filename, folder, size, uploaded FROM files WHERE hash=?",
                (sha,),
            )
            copies = db.cursor.fetchall()
            first  = copies[0]
            msg += f"📄 *{first[1]}*  —  {cnt} copies  ({_fmt(first[3])})\n"
            msg += f"🔑 `{sha[:16]}…`\n"
            for fid, name, folder, size, uploaded in copies:
                msg += f"  `{fid}` 📁 {_display_path(folder)}  ·  _{uploaded or '—'}_\n"
            msg += "\n"
    else:
        msg += "\n✅ *No content duplicates.*\n"

    # ── Filename duplicates section ────────────────────────────────────────────
    if name_dups:
        msg += f"\n━━━━━━━━━━━━━━━━━━\n"
        msg += f"📋 *Same filename, different content ({len(name_dups)} group(s)):*\n"
        msg += "_Different files with the same name in different folders._\n\n"

        for filename, cnt in name_dups:
            db.cursor.execute(
                "SELECT id, folder, size, uploaded FROM files WHERE filename=? ORDER BY uploaded",
                (filename,),
            )
            copies = db.cursor.fetchall()
            msg += f"📄 *{filename}*  —  {cnt} copies\n"
            for fid, folder, size, uploaded in copies:
                msg += f"  `{fid}` 📁 {_display_path(folder)}  ({_fmt(size)})  ·  _{uploaded or '—'}_\n"
            msg += "\n"
    else:
        msg += "\n✅ *No filename duplicates.*\n"

    # ── Summary footer ─────────────────────────────────────────────────────────
    total_wasted = 0
    for sha, cnt in content_dups:
        db.cursor.execute("SELECT size FROM files WHERE hash=? LIMIT 1", (sha,))
        row = db.cursor.fetchone()
        if row:
            total_wasted += row[0] * (cnt - 1)   # extra copies are wasted space

    if total_wasted:
        msg += f"\n━━━━━━━━━━━━━━━━━━\n"
        msg += f"💾 *Wasted space from content duplicates: {_fmt(total_wasted)}*\n"
        msg += "Use `/rm <id>` to delete the extras.\n"

    # Chunk if too long
    if len(msg) > 4000:
        chunks, chunk = [], ""
        for line in msg.split("\n"):
            if len(chunk) + len(line) + 1 > 4000:
                await update.message.reply_text(chunk, parse_mode="Markdown")
                chunk = ""
            chunk += line + "\n"
        if chunk:
            await update.message.reply_text(chunk, parse_mode="Markdown")
    else:
        await update.message.reply_text(msg, parse_mode="Markdown")


# ── /backup ───────────────────────────────────────────────────────────────────

async def _create_and_send_backup(bot, chat_id: int) -> None:
    zip_name = f"backup_{datetime.date.today()}.zip"
    try:
        with zipfile.ZipFile(zip_name, "w", compression=zipfile.ZIP_DEFLATED) as z:
            for fname in os.listdir(STORAGE):
                z.write(os.path.join(STORAGE, fname), arcname=os.path.join("storage", fname))
            if os.path.exists("storage.db"):
                z.write("storage.db", arcname="storage.db")
        with open(zip_name, "rb") as f:
            await bot.send_document(
                chat_id, f, filename=zip_name,
                caption=f"🗂️ Backup — {datetime.date.today()}",
            )
    finally:
        if os.path.exists(zip_name):
            os.remove(zip_name)


@auth
async def backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ Creating backup…")
    try:
        await _create_and_send_backup(context.bot, update.effective_chat.id)
    except Exception as exc:
        logger.error("Backup error: %s", exc)
        await update.message.reply_text("❌ Backup failed")


# ── /restore ─────────────────────────────────────────────────────────────────

@auth
async def restore_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["awaiting_restore"] = True
    await update.message.reply_text(
        "📦 *Restore mode active.*\nSend your `backup_*.zip` file now.",
        parse_mode="Markdown",
    )


async def _do_restore(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data["awaiting_restore"] = False
    doc = update.message.document

    if not doc.file_name or not doc.file_name.endswith(".zip"):
        await update.message.reply_text("❌ Send a `.zip` backup file")
        return

    await update.message.reply_text("⏳ Restoring…")

    try:
        tg = await context.bot.get_file(doc.file_id)
        data = bytes(await tg.download_as_bytearray())
    except Exception as exc:
        logger.error("Restore download: %s", exc)
        await update.message.reply_text("❌ Download failed")
        return

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "backup.zip")
        with open(zip_path, "wb") as f:
            f.write(data)
        try:
            with zipfile.ZipFile(zip_path) as z:
                z.extractall(tmpdir)
        except zipfile.BadZipFile:
            await update.message.reply_text("❌ Invalid zip")
            return

        old_db_path = os.path.join(tmpdir, "storage.db")
        if not os.path.exists(old_db_path):
            await update.message.reply_text("❌ No `storage.db` found in backup")
            return

        old_conn   = sqlite3.connect(old_db_path)
        old_cursor = old_conn.cursor()

        # Restore folders first
        old_cursor.execute("SELECT path, name, parent FROM folders")
        for path, name, parent in old_cursor.fetchall():
            db.cursor.execute(
                "INSERT OR IGNORE INTO folders(path, name, parent) VALUES(?,?,?)",
                (path, name, parent),
            )

        # Restore files
        old_cursor.execute(
            "SELECT folder, filename, filepath, key, nonce, size, hash, uploaded "
            "FROM files"
        )
        imported, skipped = 0, 0
        for folder, filename, old_path, key, nonce, size, sha, uploaded in old_cursor.fetchall():
            db.cursor.execute("SELECT id FROM files WHERE hash=?", (sha,))
            if db.cursor.fetchone():
                skipped += 1
                continue
            enc_base = os.path.basename(old_path)
            enc_src  = os.path.join(tmpdir, "storage", enc_base)
            if not os.path.exists(enc_src):
                skipped += 1
                continue
            enc_dst = os.path.join(STORAGE, enc_base)
            if os.path.exists(enc_dst):
                enc_dst = os.path.join(STORAGE, f"r_{sha[:8]}_{enc_base}")
            shutil.copy2(enc_src, enc_dst)
            db.cursor.execute(
                "INSERT INTO files(folder, filename, filepath, key, nonce, size, hash, uploaded) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (folder, filename, enc_dst, key, nonce, size, sha, uploaded),
            )
            db.conn.commit()
            db.log_action(db.cursor.lastrowid, "restore")
            imported += 1

        old_conn.close()
        db.conn.commit()

    await update.message.reply_text(
        f"✅ *Restore complete!*\n"
        f"📥 Imported: {imported}  |  ⏭️ Skipped: {skipped}",
        parse_mode="Markdown",
    )


# ── /export ───────────────────────────────────────────────────────────────────

@auth
async def export_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db.cursor.execute(
        "SELECT id, folder, filename, size, uploaded, note, hash FROM files ORDER BY folder, id"
    )
    rows = db.cursor.fetchall()

    if not rows:
        await update.message.reply_text("📭 No files to export")
        return

    buf    = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["ID", "Folder", "Filename", "Size (bytes)", "Uploaded", "Note", "SHA-256"])
    for fid, folder, name, size, uploaded, note, sha in rows:
        writer.writerow([fid, folder or "root", name, size, uploaded or "", note or "", sha])

    stream      = io.BytesIO(buf.getvalue().encode())
    stream.name = f"export_{datetime.date.today()}.csv"
    await update.message.reply_document(
        stream, filename=stream.name, caption=f"📊 {len(rows)} files exported"
    )


# ── Scheduled Sunday backup ───────────────────────────────────────────────────

async def scheduled_backup(context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.info("Running Sunday auto-backup…")
    try:
        await _create_and_send_backup(context.bot, OWNER_ID)
    except Exception as exc:
        logger.error("Auto-backup failed: %s", exc)


# ── Plain-text message — try to retrieve by ID ───────────────────────────────

@auth
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text.isdigit():
        # Proxy to /get handler
        context.args = [text]
        await get_file(update, context)
    else:
        cwd = _cwd(context)
        await update.message.reply_text(
            f"📍 Current folder: *{_display_path(cwd)}*\n\n"
            "Send a file to upload it here, or use /ls to explore.",
            parse_mode="Markdown",
        )


# ── Error handler ─────────────────────────────────────────────────────────────

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled error:", exc_info=context.error)
    if isinstance(update, Update) and update.message:
        await update.message.reply_text("❌ An unexpected error occurred. Please try again.")


# ── Bot setup ─────────────────────────────────────────────────────────────────

async def post_init(application) -> None:
    await application.bot.set_my_commands([
        BotCommand("start",   "Help & all commands"),
        BotCommand("ls",      "List current folder"),
        BotCommand("cd",      "Change folder"),
        BotCommand("pwd",     "Show current location"),
        BotCommand("tree",    "Full folder tree"),
        BotCommand("mkdir",   "Create a folder"),
        BotCommand("rmdir",   "Delete a folder + contents"),
        BotCommand("mvdir",   "Rename a folder"),
        BotCommand("get",     "Download file by ID"),
        BotCommand("info",    "File details"),
        BotCommand("note",    "Add note to a file"),
        BotCommand("mv",      "Move file to another folder"),
        BotCommand("rename",  "Rename a file"),
        BotCommand("rm",      "Delete file(s)"),
        BotCommand("find",    "Search all files"),
        BotCommand("stats",   "Storage overview"),
        BotCommand("dupes",   "Find duplicate files"),
        BotCommand("recent",  "Last 10 uploads"),
        BotCommand("backup",  "Download encrypted backup"),
        BotCommand("restore", "Restore from backup zip"),
        BotCommand("export",  "Export file list as CSV"),
    ])


def main() -> None:
    asyncio.set_event_loop(asyncio.new_event_loop())

    app = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()

    # Sunday auto-backup at 09:00 UTC
    app.job_queue.run_daily(
        scheduled_backup,
        time=datetime.time(9, 0, tzinfo=datetime.timezone.utc),
        days=(6,),
        name="sunday_backup",
    )

    app.add_handler(CommandHandler("start",   start))
    app.add_handler(CommandHandler("ls",      ls))
    app.add_handler(CommandHandler("cd",      cd))
    app.add_handler(CommandHandler("pwd",     pwd))
    app.add_handler(CommandHandler("tree",    tree))
    app.add_handler(CommandHandler("mkdir",   mkdir))
    app.add_handler(CommandHandler("rmdir",   rmdir))
    app.add_handler(CommandHandler("mvdir",   mvdir))
    app.add_handler(CommandHandler("get",     get_file))
    app.add_handler(CommandHandler("info",    info))
    app.add_handler(CommandHandler("note",    note))
    app.add_handler(CommandHandler("mv",      mv))
    app.add_handler(CommandHandler("rename",  rename_file))
    app.add_handler(CommandHandler("rm",      rm))
    app.add_handler(CommandHandler("find",    find))
    app.add_handler(CommandHandler("stats",   stats))
    app.add_handler(CommandHandler("dupes",   dupes))
    app.add_handler(CommandHandler("recent",  recent))
    app.add_handler(CommandHandler("backup",  backup))
    app.add_handler(CommandHandler("restore", restore_command))
    app.add_handler(CommandHandler("export",  export_csv))

    app.add_handler(MessageHandler(filters.PHOTO,        handle_photo))
    app.add_handler(MessageHandler(filters.VIDEO,        handle_video))
    app.add_handler(MessageHandler(filters.AUDIO,        handle_audio))
    app.add_handler(MessageHandler(filters.VOICE,        handle_voice))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    app.add_error_handler(error_handler)

    logger.info("Bot started – polling…")
    app.run_polling()


if __name__ == "__main__":
    main()
