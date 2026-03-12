"""
Personal File Storage Bot  -  Telegram-as-Storage Edition
================================================================================

HOW STORAGE WORKS
  Files are NEVER stored on the server disk.
  Instead:
    1. Your file is encrypted with AES-256-GCM
    2. The encrypted blob is sent to a private "storage channel"
    3. Only the Telegram file_id + encryption key are saved in the database
    4. When you retrieve a file, the bot downloads the blob from Telegram,
       decrypts it, and sends it back to you

  This means:
    [OK] Works on ANY free hosting (Render, Railway, Fly.io, etc.)
    [OK] No disk space needed on the server
    [OK] Files survive server restarts and redeploys forever
    [OK] Telegram stores your files for free

SETUP
  1. Create a PRIVATE Telegram channel (e.g. "My Secret Storage")
  2. Add your bot as Administrator in that channel
  3. Get the channel ID:
       - Forward any message from that channel to @userinfobot
       - It shows the chat ID (a negative number like -1001234567890)
  4. Set environment variables:
       BOT_TOKEN          = your bot token from @BotFather
       OWNER_ID           = your Telegram user ID (from @userinfobot)
       STORAGE_CHANNEL_ID = the private channel ID (e.g. -1001234567890)
       QUOTA_MB           = 0  (0 = unlimited, or set e.g. 5000 for 5 GB)
  5. pip install "python-telegram-bot[job-queue]" cryptography python-dotenv
  6. python bot.py

FOLDER STRUCTURE EXAMPLE
  [DIR] root
  |-- [DIR] Personal
  |   |-- [DIR] ID Documents
  |   |   |-- passport.pdf
  |   |   \-- aadhar_card.jpg
  |   \-- [DIR] Photos
  |-- [DIR] Education
  |   |-- [DIR] Certificates
  |   \-- notes.txt
  |-- [DIR] Medical
  \-- [DIR] Financial
      \-- [DIR] Tax

NAVIGATION (works like a terminal)
  /cd Personal/ID Documents  -> enter a folder
  /cd ..                     -> go up
  /cd /                      -> go to root
  /ls                        -> list current folder
  /tree                      -> see full structure
================================================================================
"""

import asyncio
import csv
import datetime
import hashlib
import io
import logging
import os
import sqlite3
import tempfile
import zipfile
from functools import wraps

from dotenv import load_dotenv
load_dotenv()

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


# -- Logging -------------------------------------------------------------------

logging.basicConfig(
    format="%(asctime)s  %(levelname)s  %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# -- Config --------------------------------------------------------------------

BOT_TOKEN          = os.getenv("BOT_TOKEN")
OWNER_ID           = int(os.getenv("OWNER_ID", "0"))
STORAGE_CHANNEL_ID = int(os.getenv("STORAGE_CHANNEL_ID", "0"))
QUOTA_BYTES        = int(os.getenv("QUOTA_MB", "0")) * 1024 * 1024   # 0 = unlimited

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable is not set")
if not OWNER_ID:
    raise RuntimeError("OWNER_ID environment variable is not set")
if not STORAGE_CHANNEL_ID:
    raise RuntimeError(
        "STORAGE_CHANNEL_ID is not set.\n"
        "Create a private Telegram channel, add the bot as admin, "
        "then set STORAGE_CHANNEL_ID to its chat ID."
    )

MAX_FILE_SIZE     = 20 * 1024 * 1024
IMAGE_EXTENSIONS  = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}
MIME_TO_EXT       = {
    "image/jpeg": ".jpg", "image/png": ".png", "image/gif": ".gif",
    "image/webp": ".webp", "video/mp4": ".mp4", "audio/mpeg": ".mp3",
    "audio/ogg": ".ogg",
}


# -- Auth ----------------------------------------------------------------------

def auth(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != OWNER_ID:
            await update.message.reply_text("[BLOCKED] Private bot.")
            return
        return await func(update, context)
    return wrapper


# -- Current-folder state ------------------------------------------------------

def _cwd(context: ContextTypes.DEFAULT_TYPE) -> str:
    return context.user_data.get("cwd", "")

def _set_cwd(context: ContextTypes.DEFAULT_TYPE, path: str) -> None:
    context.user_data["cwd"] = path

def _display_path(path: str) -> str:
    return f"[DIR] {path}" if path else "[DIR] root"


# -- Utilities -----------------------------------------------------------------

def _fmt(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"

def _dated(base: str, ext: str) -> str:
    return f"{base}_{datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S')}{ext}"

def _resolve(context: ContextTypes.DEFAULT_TYPE, user_input: str) -> str:
    raw = user_input.strip()
    if raw in ("/", ""):
        return ""
    if raw.startswith("/"):
        return db.normalise(raw)
    if raw == "..":
        cwd = _cwd(context)
        return cwd.rsplit("/", 1)[0] if "/" in cwd else ""
    cwd = _cwd(context)
    return db.normalise((cwd + "/" + raw) if cwd else raw)

def _build_tree(path: str, prefix: str = "", depth: int = 0, max_depth: int = 5) -> str:
    if depth > max_depth:
        return ""
    result = db.list_folder(path)
    items  = [("folder", sf[0], sf[1]) for sf in result["subfolders"]] + \
             [("file",   str(f[0]), f[1]) for f in result["files"]]
    lines  = []
    for i, (kind, fid_or_path, name) in enumerate(items):
        is_last   = (i == len(items) - 1)
        connector = "\-- " if is_last else "|-- "
        extender  = "    " if is_last else "|   "
        if kind == "folder":
            lines.append(f"{prefix}{connector}[DIR] {name}")
            lines.append(_build_tree(fid_or_path, prefix + extender, depth + 1, max_depth))
        else:
            db.cursor.execute("SELECT size FROM files WHERE id=?", (fid_or_path,))
            row = db.cursor.fetchone()
            size_str = f"  ({_fmt(row[0])})" if row else ""
            lines.append(f"{prefix}{connector}[FILE] {name}{size_str}")
    return "\n".join(l for l in lines if l)

def _quota_warn() -> str:
    if not QUOTA_BYTES:
        return ""
    used = db.total_used_bytes()
    pct  = used / QUOTA_BYTES * 100
    if pct >= 80:
        return f"\n[WARN] *Quota:* {pct:.1f}% used ({_fmt(used)} / {_fmt(QUOTA_BYTES)})"
    return ""


# -- /start --------------------------------------------------------------------

@auth
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cwd = _cwd(context)
    await update.message.reply_text(
        "[DIR] *Personal File Storage Bot*\n"
        "_(Files stored securely inside Telegram - no server disk used)_\n\n"
        f"[PATH] Current folder: {_display_path(cwd)}\n\n"
        "------------------\n"
        "[DIR] *NAVIGATION*\n"
        "------------------\n"
        "`/ls` - list current folder\n"
        "`/ls <folder>` - list any folder\n"
        "`/cd <folder>` - enter a folder\n"
        "`/cd ..` - go up  |  `/cd /` - go to root\n"
        "`/tree` - full folder tree\n"
        "`/pwd` - show current location\n\n"
        "------------------\n"
        "[DIR] *FOLDERS*\n"
        "------------------\n"
        "`/mkdir <n>` - create folder (nested OK)\n"
        "`/rmdir <folder>` - delete folder + contents\n"
        "`/mvdir <folder> <new name>` - rename folder\n\n"
        "------------------\n"
        "[FILE] *FILES*\n"
        "------------------\n"
        "Send any file -> saved to current folder\n"
        "`/get <id>` - download file\n"
        "`/info <id>` - file details\n"
        "`/note <id> <text>` - add note\n"
        "`/mv <id> <folder>` - move file\n"
        "`/rename <id> <new name>` - rename file\n"
        "`/rm <id>` - delete file(s)\n"
        "`/find <n>` - search all files\n\n"
        "------------------\n"
        "[STATS] *MISC*\n"
        "------------------\n"
        "`/stats` - storage overview\n"
        "`/dupes` - find duplicate files\n"
        "`/recent` - last 10 uploads\n"
        "`/backup` - backup database\n"
        "`/restore` - restore from backup\n"
        "`/export` - export file list as CSV\n",
        parse_mode="Markdown",
    )


# -- /pwd ----------------------------------------------------------------------

@auth
async def pwd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"[PATH] You are in: *{_display_path(_cwd(context))}*",
        parse_mode="Markdown",
    )


# -- /cd -----------------------------------------------------------------------

@auth
async def cd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "Usage: `/cd <folder>` | `/cd ..` | `/cd /`", parse_mode="Markdown"
        )
        return
    target = " ".join(context.args)
    path   = _resolve(context, target)
    if not db.folder_exists(path):
        await update.message.reply_text(
            f"[ERROR] Folder `{path or 'root'}` does not exist.", parse_mode="Markdown"
        )
        return
    _set_cwd(context, path)
    content = db.list_folder(path)
    await update.message.reply_text(
        f"[DIR] *{_display_path(path)}*\n"
        f"{len(content['subfolders'])} folder(s) | {len(content['files'])} file(s)\n\n"
        "Use /ls to see contents.",
        parse_mode="Markdown",
    )


# -- /ls -----------------------------------------------------------------------

@auth
async def ls(update: Update, context: ContextTypes.DEFAULT_TYPE):
    path = _resolve(context, " ".join(context.args)) if context.args else _cwd(context)
    if not db.folder_exists(path):
        await update.message.reply_text(f"[ERROR] Folder `{path}` not found.", parse_mode="Markdown")
        return
    content    = db.list_folder(path)
    subfolders = content["subfolders"]
    files      = content["files"]
    lines      = [f"[DIR] *{_display_path(path)}*\n"]
    if not subfolders and not files:
        lines.append("_Empty folder._\n\nSend a file to upload here, or /mkdir to create a subfolder.")
    else:
        if subfolders:
            lines.append("*Folders:*")
            for fpath, fname in subfolders:
                sub = db.list_folder(fpath)
                lines.append(
                    f"  [DIR] *{fname}*  >  {len(sub['subfolders'])} folder(s), {len(sub['files'])} file(s)"
                )
        if files:
            if subfolders:
                lines.append("")
            lines.append("*Files:*")
            for fid, name, size, uploaded in files:
                lines.append(f"  `{fid}` [FILE] {name}  ({_fmt(size)})  _{uploaded or '-'}_")
    msg = "\n".join(lines)
    if len(msg) > 4000:
        for i in range(0, len(lines), 50):
            await update.message.reply_text("\n".join(lines[i:i+50]), parse_mode="Markdown")
    else:
        await update.message.reply_text(msg, parse_mode="Markdown")


# -- /tree ---------------------------------------------------------------------

@auth
async def tree(update: Update, context: ContextTypes.DEFAULT_TYPE):
    path = _resolve(context, " ".join(context.args)) if context.args else ""
    if not db.folder_exists(path):
        await update.message.reply_text(f"[ERROR] Folder `{path}` not found.", parse_mode="Markdown")
        return
    body = _build_tree(path)
    msg  = f"[TREE] *Tree: {_display_path(path)}*\n\n"
    msg += f"```\n{body}\n```" if body else "_Empty._"
    if len(msg) > 4000:
        msg = msg[:3950] + "\n..._(truncated, use /ls to browse)_"
    await update.message.reply_text(msg, parse_mode="Markdown")


# -- /mkdir --------------------------------------------------------------------

@auth
async def mkdir(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "Usage: `/mkdir <n>` or `/mkdir Personal/ID Docs`", parse_mode="Markdown"
        )
        return
    raw  = " ".join(context.args)
    cwd  = _cwd(context)
    path = db.normalise((cwd + "/" + raw) if (cwd and not raw.startswith("/")) else raw)
    if db.folder_exists(path):
        await update.message.reply_text(f"[INFO] Folder *{path}* already exists.", parse_mode="Markdown")
        return
    err = db.create_folder(path)
    if err:
        await update.message.reply_text(f"[ERROR] {err}")
        return
    await update.message.reply_text(
        f"[OK] Folder *{path}* created!\nUse `/cd {path.split('/')[-1]}` to enter it.",
        parse_mode="Markdown",
    )


# -- /rmdir --------------------------------------------------------------------

@auth
async def rmdir(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/rmdir <folder>`", parse_mode="Markdown")
        return
    path = _resolve(context, " ".join(context.args))
    if not db.folder_exists(path) or path == "":
        await update.message.reply_text(
            f"[ERROR] Folder `{path or 'root'}` not found or cannot delete root.",
            parse_mode="Markdown",
        )
        return

    # Delete messages from the storage channel first
    msg_ids = db.get_tg_msg_ids_in_folder(path)
    deleted_msgs = 0
    for mid in msg_ids:
        try:
            await context.bot.delete_message(STORAGE_CHANNEL_ID, mid)
            deleted_msgs += 1
        except Exception:
            pass

    folders_del, files_del = db.delete_folder(path)

    cwd = _cwd(context)
    if cwd == path or cwd.startswith(path + "/"):
        _set_cwd(context, path.rsplit("/", 1)[0] if "/" in path else "")

    await update.message.reply_text(
        f"[DELETE] Deleted folder *{path}*\n"
        f"Removed {folders_del} folder(s), {files_del} file(s).",
        parse_mode="Markdown",
    )


# -- /mvdir --------------------------------------------------------------------

@auth
async def mvdir(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text(
            "Usage: `/mvdir <folder> <new name>`", parse_mode="Markdown"
        )
        return
    old_path = _resolve(context, context.args[0])
    new_name = " ".join(context.args[1:])
    err = db.rename_folder(old_path, new_name)
    if err:
        await update.message.reply_text(f"[ERROR] {err}", parse_mode="Markdown")
        return
    parent   = "/".join(old_path.split("/")[:-1])
    new_path = (parent + "/" + new_name).strip("/")
    if _cwd(context) == old_path:
        _set_cwd(context, new_path)
    await update.message.reply_text(
        f"[RENAME] Renamed *{old_path}* -> *{new_path}*", parse_mode="Markdown"
    )


# -- Core upload - sends encrypted blob to storage channel ---------------------

async def _do_upload(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    tg_file_id: str,
    filename: str,
    file_size: int | None,
) -> None:
    cwd = _cwd(context)

    if file_size and file_size > MAX_FILE_SIZE:
        await update.message.reply_text("[ERROR] File too large (max 20 MB per file)")
        return

    if QUOTA_BYTES and file_size and (db.total_used_bytes() + file_size) > QUOTA_BYTES:
        await update.message.reply_text(
            f"[ERROR] Storage quota full ({_fmt(db.total_used_bytes())} / {_fmt(QUOTA_BYTES)}).\n"
            "Delete some files with /rm."
        )
        return

    await update.message.reply_text(
        f"[WAIT] Uploading *{filename}* -> {_display_path(cwd)}...",
        parse_mode="Markdown",
    )

    # -- Download original file from Telegram ---------------------------------
    try:
        tg = await context.bot.get_file(tg_file_id)
        data = bytes(await tg.download_as_bytearray())
    except Exception as exc:
        logger.error("Download error: %s", exc)
        await update.message.reply_text("[ERROR] Failed to download from Telegram")
        return

    sha = hashlib.sha256(data).hexdigest()

    # -- Duplicate check -------------------------------------------------------
    db.cursor.execute("SELECT id, folder FROM files WHERE hash=-", (sha,))
    existing = db.cursor.fetchone()
    if existing:
        await update.message.reply_text(
            f"[WARN] Duplicate - identical file already stored as ID `{existing[0]}` "
            f"in {_display_path(existing[1])}",
            parse_mode="Markdown",
        )
        return

    # -- Encrypt ---------------------------------------------------------------
    try:
        encrypted, nonce, key = encrypt_file(data)
    except Exception as exc:
        logger.error("Encrypt error: %s", exc)
        await update.message.reply_text("[ERROR] Encryption failed")
        return

    # -- Send encrypted blob to the private storage channel -------------------
    try:
        enc_stream = io.BytesIO(encrypted)
        enc_stream.name = f"{sha[:16]}.enc"   # opaque name in the channel
        sent = await context.bot.send_document(
            chat_id  = STORAGE_CHANNEL_ID,
            document = enc_stream,
            filename = enc_stream.name,
        )
        stored_tg_file_id = sent.document.file_id
        stored_tg_msg_id  = sent.message_id
    except Exception as exc:
        logger.error("Storage channel upload error: %s", exc)
        await update.message.reply_text(
            "[ERROR] Failed to save to storage channel.\n"
            "Make sure the bot is an Admin in your storage channel and "
            "STORAGE_CHANNEL_ID is correct."
        )
        return

    # -- Save metadata to DB ---------------------------------------------------
    try:
        db.cursor.execute(
            "INSERT INTO files(folder, filename, tg_file_id, tg_msg_id, key, nonce, size, hash, uploaded) "
            "VALUES(-,-,-,-,-,-,-,-,datetime('now'))",
            (cwd, filename, stored_tg_file_id, stored_tg_msg_id, key, nonce, len(data), sha),
        )
        new_id = db.cursor.lastrowid
        db.conn.commit()
    except Exception as exc:
        logger.error("DB insert error: %s", exc)
        await update.message.reply_text("[ERROR] Database error")
        return

    db.log_action(new_id, "upload")

    await update.message.reply_text(
        f"[OK] *Saved!*\n"
        f"[FILE] {filename}\n"
        f"[ID] ID: `{new_id}`\n"
        f"[DIR] Folder: {_display_path(cwd)}\n"
        f"[SIZE] Size: {_fmt(len(data))}"
        + _quota_warn(),
        parse_mode="Markdown",
    )


# -- Upload handlers -----------------------------------------------------------

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
    await _do_upload(update, context, photo.file_id,
                     _dated(caption or "photo", ".jpg"), photo.file_size)

@auth
async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    video = update.message.video
    ext   = MIME_TO_EXT.get(video.mime_type or "", ".mp4")
    await _do_upload(update, context, video.file_id,
                     video.file_name or _dated("video", ext), video.file_size)

@auth
async def handle_audio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    audio = update.message.audio
    ext   = MIME_TO_EXT.get(audio.mime_type or "", ".mp3")
    await _do_upload(update, context, audio.file_id,
                     audio.file_name or _dated("audio", ext), audio.file_size)

@auth
async def handle_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _do_upload(update, context, update.message.voice.file_id,
                     _dated("voice", ".ogg"), update.message.voice.file_size)


# -- /get ---------------------------------------------------------------------

@auth
async def get_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: `/get <id>`", parse_mode="Markdown")
        return

    file_id = int(context.args[0])
    db.cursor.execute(
        "SELECT tg_file_id, key, nonce, filename FROM files WHERE id=-", (file_id,)
    )
    row = db.cursor.fetchone()
    if not row:
        await update.message.reply_text(f"[ERROR] No file with ID `{file_id}`", parse_mode="Markdown")
        return

    tg_fid, key, nonce, filename = row

    await update.message.reply_text(
        f"[WAIT] Decrypting *{filename}*...", parse_mode="Markdown"
    )

    # -- Download encrypted blob from storage channel --------------------------
    try:
        tg      = await context.bot.get_file(tg_fid)
        enc     = bytes(await tg.download_as_bytearray())
        data    = decrypt_file(enc, nonce, key)
    except Exception as exc:
        logger.error("Retrieve/decrypt error: %s", exc)
        await update.message.reply_text(
            "[ERROR] Failed to retrieve or decrypt the file.\n"
            "The storage channel message may have been deleted manually."
        )
        return

    # -- Image preview ---------------------------------------------------------
    ext = os.path.splitext(filename)[1].lower()
    if ext in IMAGE_EXTENSIONS:
        try:
            await update.message.reply_photo(
                io.BytesIO(data), caption=f"[PHOTO] *{filename}*", parse_mode="Markdown"
            )
        except Exception:
            pass

    stream      = io.BytesIO(data)
    stream.name = filename
    await update.message.reply_document(stream, filename=filename)
    db.log_action(file_id, "download")


# -- /info ---------------------------------------------------------------------

@auth
async def info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: `/info <id>`", parse_mode="Markdown")
        return
    file_id = int(context.args[0])
    db.cursor.execute(
        "SELECT filename, folder, size, hash, uploaded, note FROM files WHERE id=-", (file_id,)
    )
    row = db.cursor.fetchone()
    if not row:
        await update.message.reply_text(f"[ERROR] No file with ID `{file_id}`", parse_mode="Markdown")
        return
    name, folder, size, sha, uploaded, note = row
    db.cursor.execute(
        "SELECT action, ts FROM access_log WHERE file_id=- ORDER BY ts DESC LIMIT 5", (file_id,)
    )
    logs = "\n".join(
        f"  {'[UP]' if a == 'upload' else '[DOWN]'} `{a}` - {t}"
        for a, t in db.cursor.fetchall()
    ) or "  _none_"
    await update.message.reply_text(
        f"[FILE] *{name}*\n"
        f"[ID] ID: `{file_id}`\n"
        f"[DIR] Folder: {_display_path(folder)}\n"
        f"[SIZE] Size: {_fmt(size)}\n"
        f"[TIME] Uploaded: {uploaded or '-'}\n"
        f"[NOTE] Note: {note or '_none_'}\n"
        f"[HASH] SHA-256:\n`{sha}`\n\n"
        f"[LOG] *Recent access:*\n{logs}",
        parse_mode="Markdown",
    )


# -- /note ---------------------------------------------------------------------

@auth
async def note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2 or not context.args[0].isdigit():
        await update.message.reply_text("Usage: `/note <id> <text>`", parse_mode="Markdown")
        return
    db.cursor.execute("UPDATE files SET note=- WHERE id=-",
                      (" ".join(context.args[1:]), int(context.args[0])))
    db.conn.commit()
    await update.message.reply_text(
        f"[NOTE] Note saved for `{context.args[0]}`", parse_mode="Markdown"
    )


# -- /mv -----------------------------------------------------------------------

@auth
async def mv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2 or not context.args[0].isdigit():
        await update.message.reply_text(
            "Usage: `/mv <id> <folder>`", parse_mode="Markdown"
        )
        return
    file_id = int(context.args[0])
    dest    = _resolve(context, " ".join(context.args[1:]))
    db.cursor.execute("SELECT filename FROM files WHERE id=-", (file_id,))
    row = db.cursor.fetchone()
    if not row:
        await update.message.reply_text(f"[ERROR] No file with ID `{file_id}`", parse_mode="Markdown")
        return
    err = db.move_file(file_id, dest)
    if err:
        await update.message.reply_text(f"[ERROR] {err}", parse_mode="Markdown")
        return
    await update.message.reply_text(
        f"[SIZE] *{row[0]}* moved to {_display_path(dest)}", parse_mode="Markdown"
    )


# -- /rename -------------------------------------------------------------------

@auth
async def rename_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2 or not context.args[0].isdigit():
        await update.message.reply_text(
            "Usage: `/rename <id> <new name>`", parse_mode="Markdown"
        )
        return
    file_id  = int(context.args[0])
    new_name = " ".join(context.args[1:])
    db.cursor.execute("SELECT filename FROM files WHERE id=-", (file_id,))
    row = db.cursor.fetchone()
    if not row:
        await update.message.reply_text(f"[ERROR] No file with ID `{file_id}`", parse_mode="Markdown")
        return
    db.cursor.execute("UPDATE files SET filename=- WHERE id=-", (new_name, file_id))
    db.conn.commit()
    await update.message.reply_text(
        f"[RENAME] Renamed *{row[0]}* -> *{new_name}*", parse_mode="Markdown"
    )


# -- /rm -----------------------------------------------------------------------

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
        db.cursor.execute("SELECT tg_msg_id, filename FROM files WHERE id=-", (fid,))
        row = db.cursor.fetchone()
        if not row:
            errors.append(f"ID `{fid}` not found")
            continue
        tg_msg_id, filename = row
        # Delete the message from storage channel
        if tg_msg_id:
            try:
                await context.bot.delete_message(STORAGE_CHANNEL_ID, tg_msg_id)
            except Exception as exc:
                logger.warning("Could not delete storage channel message %s: %s", tg_msg_id, exc)
        db.cursor.execute("DELETE FROM files WHERE id=-", (fid,))
        db.conn.commit()
        deleted.append(f"`{fid}` {filename}")

    msg = ""
    if deleted:
        msg += "[DELETE] *Deleted:*\n" + "\n".join(deleted)
    if errors:
        msg += ("\n\n" if msg else "") + "[ERROR] *Errors:*\n" + "\n".join(errors)
    await update.message.reply_text(msg or "Nothing deleted.", parse_mode="Markdown")


# -- /find ---------------------------------------------------------------------

@auth
async def find(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/find <n>`", parse_mode="Markdown")
        return
    query = " ".join(context.args)
    db.cursor.execute(
        "SELECT id, filename, folder, size, uploaded FROM files "
        "WHERE filename LIKE - ORDER BY folder, filename",
        (f"%{query}%",),
    )
    rows = db.cursor.fetchall()
    if not rows:
        await update.message.reply_text(
            f"[SEARCH] No files matching *{query}*", parse_mode="Markdown"
        )
        return
    msg = f"[SEARCH] *Results for \"{query}\" ({len(rows)}):*\n\n"
    for fid, name, folder, size, uploaded in rows:
        msg += (
            f"`{fid}` [FILE] {name}  ({_fmt(size)})\n"
            f"       [DIR] {_display_path(folder)}\n"
        )
    if len(msg) > 4000:
        msg = msg[:3950] + "\n..._(truncated)_"
    await update.message.reply_text(msg, parse_mode="Markdown")


# -- /dupes --------------------------------------------------------------------

@auth
async def dupes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db.cursor.execute(
        "SELECT hash, COUNT(*) as cnt FROM files GROUP BY hash HAVING cnt > 1 ORDER BY cnt DESC"
    )
    content_dups = db.cursor.fetchall()

    db.cursor.execute(
        "SELECT filename, COUNT(*) as cnt FROM files GROUP BY filename HAVING cnt > 1 ORDER BY cnt DESC"
    )
    name_dups = db.cursor.fetchall()

    if not content_dups and not name_dups:
        await update.message.reply_text(
            "[OK] *No duplicates found!*\nEvery file has unique content and a unique filename.",
            parse_mode="Markdown",
        )
        return

    msg = "[SEARCH] *Duplicate File Report*\n"

    if content_dups:
        msg += f"\n------------------\n[DUPES] *Identical content ({len(content_dups)} group(s)):*\n"
        msg += "_Safe to delete the extras with /rm_\n\n"
        for sha, cnt in content_dups:
            db.cursor.execute(
                "SELECT id, filename, folder, size FROM files WHERE hash=-", (sha,)
            )
            copies = db.cursor.fetchall()
            msg += f"[FILE] *{copies[0][1]}* - {cnt} copies  ({_fmt(copies[0][3])})\n"
            msg += f"[HASH] `{sha[:16]}...`\n"
            for fid, name, folder, _ in copies:
                msg += f"  `{fid}` {_display_path(folder)}\n"
            msg += "\n"
    else:
        msg += "\n[OK] No content duplicates.\n"

    if name_dups:
        msg += f"\n------------------\n[LOG] *Same filename, different content ({len(name_dups)} group(s)):*\n\n"
        for filename, cnt in name_dups:
            db.cursor.execute(
                "SELECT id, folder, size, uploaded FROM files WHERE filename=- ORDER BY uploaded",
                (filename,),
            )
            copies = db.cursor.fetchall()
            msg += f"[FILE] *{filename}* - {cnt} copies\n"
            for fid, folder, size, uploaded in copies:
                msg += f"  `{fid}` {_display_path(folder)}  ({_fmt(size)})\n"
            msg += "\n"
    else:
        msg += "\n[OK] No filename duplicates.\n"

    # Wasted space
    wasted = sum(
        db.cursor.execute("SELECT size FROM files WHERE hash=- LIMIT 1", (sha,)).fetchone()[0]
        * (cnt - 1)
        for sha, cnt in content_dups
    )
    if wasted:
        msg += f"\n------------------\n[SPACE] *Wasted space: {_fmt(wasted)}*\nUse `/rm <id>` to delete extras.\n"

    if len(msg) > 4000:
        msg = msg[:3950] + "\n..._(truncated)_"
    await update.message.reply_text(msg, parse_mode="Markdown")


# -- /stats --------------------------------------------------------------------

@auth
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db.cursor.execute("SELECT COUNT(*), COALESCE(SUM(size),0) FROM files")
    total_files, total_size = db.cursor.fetchone()
    db.cursor.execute("SELECT COUNT(*) FROM folders")
    total_folders = db.cursor.fetchone()[0]
    db.cursor.execute("SELECT filename, size FROM files ORDER BY size DESC LIMIT 1")
    largest = db.cursor.fetchone()
    db.cursor.execute("SELECT filename, uploaded FROM files ORDER BY uploaded DESC LIMIT 1")
    newest = db.cursor.fetchone()

    quota_line = ""
    if QUOTA_BYTES:
        pct    = total_size / QUOTA_BYTES * 100
        filled = int(20 * min(pct, 100) / 100)
        bar    = "#" * filled + "-" * (20 - filled)
        quota_line = f"\n[{bar}] {pct:.1f}%\n"

    db.cursor.execute(
        "SELECT folder, COUNT(*), SUM(size) FROM files GROUP BY folder ORDER BY folder"
    )
    folder_rows = db.cursor.fetchall()

    msg = (
        f"[STATS] *Storage Stats*\n\n"
        f"[STORAGE] Storage: Telegram Channel\n"
        f"[DIR] Folders: {total_folders}\n"
        f"[FILE] Files: {total_files}\n"
        f"[SPACE] Total size: {_fmt(total_size)}"
        + (f" / {_fmt(QUOTA_BYTES)}" if QUOTA_BYTES else " _(unlimited)_")
        + quota_line
    )
    if largest:
        msg += f"\n[SIZE] Largest: *{largest[0]}*  ({_fmt(largest[1])})"
    if newest:
        msg += f"\n[TIME] Newest: *{newest[0]}*  ({newest[1] or '-'})"
    if folder_rows:
        msg += "\n\n*By folder:*\n"
        for folder, cnt, fsize in folder_rows:
            msg += f"  {_display_path(folder)}: {cnt} file(s), {_fmt(fsize)}\n"

    await update.message.reply_text(msg, parse_mode="Markdown")


# -- /recent -------------------------------------------------------------------

@auth
async def recent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db.cursor.execute(
        "SELECT id, filename, folder, size, uploaded FROM files ORDER BY uploaded DESC LIMIT 10"
    )
    rows = db.cursor.fetchall()
    if not rows:
        await update.message.reply_text("[EMPTY] No files yet.")
        return
    msg = "[TIME] *Last 10 uploads:*\n\n"
    for fid, name, folder, size, uploaded in rows:
        msg += (
            f"`{fid}` [FILE] *{name}*  ({_fmt(size)})\n"
            f"       [DIR] {_display_path(folder)}  |  _{uploaded or '-'}_\n"
        )
    await update.message.reply_text(msg, parse_mode="Markdown")


# -- /backup - sends only the SQLite DB (files are already safe in Telegram) ---

@auth
async def backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("[WAIT] Creating backup...")
    try:
        with open("storage.db", "rb") as f:
            await update.message.reply_document(
                f,
                filename=f"storage_backup_{datetime.date.today()}.db",
                caption=(
                    f"[BACKUP] *Database Backup - {datetime.date.today()}*\n\n"
                    "This file contains all your folder structure, "
                    "file metadata and encryption keys.\n"
                    "Your actual files are safely stored in your Telegram storage channel."
                ),
                parse_mode="Markdown",
            )
    except Exception as exc:
        logger.error("Backup error: %s", exc)
        await update.message.reply_text("[ERROR] Backup failed")


# -- Scheduled Sunday backup ---------------------------------------------------

async def scheduled_backup(context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.info("Running Sunday auto-backup...")
    try:
        with open("storage.db", "rb") as f:
            await context.bot.send_document(
                OWNER_ID,
                f,
                filename=f"storage_backup_{datetime.date.today()}.db",
                caption=f"[BACKUP] Auto Backup - {datetime.date.today()}",
            )
    except Exception as exc:
        logger.error("Auto-backup failed: %s", exc)


# -- /restore - re-import metadata from a DB backup ---------------------------

@auth
async def restore_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["awaiting_restore"] = True
    await update.message.reply_text(
        "[SIZE] *Restore mode active.*\nSend your `storage_backup_*.db` file now.",
        parse_mode="Markdown",
    )


async def _do_restore(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data["awaiting_restore"] = False
    doc = update.message.document

    if not doc.file_name or not doc.file_name.endswith(".db"):
        await update.message.reply_text("[ERROR] Please send a `.db` backup file")
        return

    await update.message.reply_text("[WAIT] Restoring from backup...")

    try:
        tg   = await context.bot.get_file(doc.file_id)
        data = bytes(await tg.download_as_bytearray())
    except Exception as exc:
        logger.error("Restore download: %s", exc)
        await update.message.reply_text("[ERROR] Download failed")
        return

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp.write(data)
        tmp_path = tmp.name

    try:
        old_conn   = sqlite3.connect(tmp_path)
        old_cursor = old_conn.cursor()

        # Restore folders
        old_cursor.execute("SELECT path, name, parent FROM folders")
        for path, name, parent in old_cursor.fetchall():
            db.cursor.execute(
                "INSERT OR IGNORE INTO folders(path, name, parent) VALUES(-,-,-)",
                (path, name, parent),
            )

        # Restore files
        old_cursor.execute(
            "SELECT folder, filename, tg_file_id, tg_msg_id, key, nonce, size, hash, uploaded "
            "FROM files"
        )
        imported, skipped = 0, 0
        for folder, filename, tg_fid, tg_msg, key, nonce, size, sha, uploaded in old_cursor.fetchall():
            db.cursor.execute("SELECT id FROM files WHERE hash=-", (sha,))
            if db.cursor.fetchone():
                skipped += 1
                continue
            db.cursor.execute(
                "INSERT INTO files(folder, filename, tg_file_id, tg_msg_id, key, nonce, size, hash, uploaded) "
                "VALUES(-,-,-,-,-,-,-,-,-)",
                (folder, filename, tg_fid, tg_msg, key, nonce, size, sha, uploaded),
            )
            db.conn.commit()
            db.log_action(db.cursor.lastrowid, "restore")
            imported += 1

        old_conn.close()
        db.conn.commit()
    finally:
        os.unlink(tmp_path)

    await update.message.reply_text(
        f"[OK] *Restore complete!*\n"
        f"[IMPORTED] Imported: {imported}  |  [SKIPPED] Skipped: {skipped}",
        parse_mode="Markdown",
    )


# -- /export -------------------------------------------------------------------

@auth
async def export_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db.cursor.execute(
        "SELECT id, folder, filename, size, uploaded, note, hash FROM files ORDER BY folder, id"
    )
    rows = db.cursor.fetchall()
    if not rows:
        await update.message.reply_text("[EMPTY] No files to export")
        return
    buf    = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["ID", "Folder", "Filename", "Size (bytes)", "Uploaded", "Note", "SHA-256"])
    for fid, folder, name, size, uploaded, note, sha in rows:
        writer.writerow([fid, folder or "root", name, size, uploaded or "", note or "", sha])
    stream      = io.BytesIO(buf.getvalue().encode())
    stream.name = f"export_{datetime.date.today()}.csv"
    await update.message.reply_document(
        stream, filename=stream.name, caption=f"[STATS] {len(rows)} files exported"
    )


# -- Plain-text message --------------------------------------------------------

@auth
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text.isdigit():
        context.args = [text]
        await get_file(update, context)
    else:
        await update.message.reply_text(
            f"[PATH] Current folder: *{_display_path(_cwd(context))}*\n\n"
            "Send a file to upload it here, or use /ls to explore.",
            parse_mode="Markdown",
        )


# -- Error handler -------------------------------------------------------------

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled error:", exc_info=context.error)
    if isinstance(update, Update) and update.message:
        await update.message.reply_text("[ERROR] An unexpected error occurred. Please try again.")


# -- Bot setup -----------------------------------------------------------------

async def post_init(application) -> None:
    await application.bot.set_my_commands([
        BotCommand("start",   "Help & all commands"),
        BotCommand("ls",      "List current folder"),
        BotCommand("cd",      "Change folder"),
        BotCommand("pwd",     "Show current location"),
        BotCommand("tree",    "Full folder tree"),
        BotCommand("mkdir",   "Create a folder"),
        BotCommand("rmdir",   "Delete folder + contents"),
        BotCommand("mvdir",   "Rename a folder"),
        BotCommand("get",     "Download file by ID"),
        BotCommand("info",    "File details"),
        BotCommand("note",    "Add note to a file"),
        BotCommand("mv",      "Move file to another folder"),
        BotCommand("rename",  "Rename a file"),
        BotCommand("rm",      "Delete file(s)"),
        BotCommand("find",    "Search all files"),
        BotCommand("dupes",   "Find duplicate files"),
        BotCommand("stats",   "Storage overview"),
        BotCommand("recent",  "Last 10 uploads"),
        BotCommand("backup",  "Download DB backup"),
        BotCommand("restore", "Restore from DB backup"),
        BotCommand("export",  "Export file list as CSV"),
    ])


def main() -> None:
    asyncio.set_event_loop(asyncio.new_event_loop())
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()

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
    app.add_handler(CommandHandler("dupes",   dupes))
    app.add_handler(CommandHandler("stats",   stats))
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

    logger.info("Bot started - polling... (Storage: Telegram Channel)")
    app.run_polling()


if __name__ == "__main__":
    main()
