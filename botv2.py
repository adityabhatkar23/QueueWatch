import logging
import os
import re
import secrets
import subprocess
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

import psycopg2
import psycopg2.extras
import requests
import uvicorn
from fastapi import FastAPI, HTTPException
from telegram import Update, BotCommand
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from telegram.request import HTTPXRequest

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("slurm_bot")


# ── Config ────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Config:
    bot_token: str
    database_url: str
    log_dir: str = "."
    api_host: str = "0.0.0.0"
    api_port: int = int(os.environ.get("PORT", 8000))  # Railway injects PORT


def load_config() -> Config:
    token = os.environ.get("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN env var is required")
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL env var is required")
    return Config(bot_token=token, database_url=database_url)


CFG = load_config()


# ── Database ──────────────────────────────────────────────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    telegram_id BIGINT PRIMARY KEY,
    username    TEXT,
    token       TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS jobs (
    job_id      TEXT,
    telegram_id BIGINT REFERENCES users(telegram_id) ON DELETE CASCADE,
    status      TEXT,
    UNIQUE(job_id, telegram_id)
);

CREATE TABLE IF NOT EXISTS logs (
    id        SERIAL PRIMARY KEY,
    job_id    TEXT,
    message   TEXT,
    ts        TIMESTAMPTZ DEFAULT NOW()
);
"""


def init_db() -> None:
    with db() as cur:
        cur.execute(SCHEMA)
    log.info("Database schema ready.")


@contextmanager
def db():
    conn = psycopg2.connect(CFG.database_url, connect_timeout=10)
    conn.autocommit = False
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Helpers ───────────────────────────────────────────────────────────────────
_SAFE_JOB_ID = re.compile(r"^\w[\w.\-]{0,63}$")


def safe_job_id(job_id: str) -> str:
    if not _SAFE_JOB_ID.match(job_id):
        raise ValueError(f"Invalid job_id: {job_id!r}")
    return job_id


def send_telegram(tid: int, message: str) -> None:
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{CFG.bot_token}/sendMessage",
            data={"chat_id": tid, "text": message},
            timeout=10,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.warning("Failed to notify %s: %s", tid, exc)


def _verify_token(token: str) -> int:
    """Return telegram_id for token or raise 401."""
    with db() as cur:
        cur.execute("SELECT telegram_id FROM users WHERE token = %s", (token,))
        row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=401, detail="Invalid token")
    return row["telegram_id"]


# ── Bot commands ──────────────────────────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    with db() as cur:
        cur.execute(
            """
            INSERT INTO users (telegram_id, username)
            VALUES (%s, %s)
            ON CONFLICT (telegram_id) DO UPDATE SET username = EXCLUDED.username
            """,
            (user.id, user.username or "N/A"),
        )
    await update.message.reply_text(
        "👋 Welcome to QueueWatch!\n\n"
        "Get Telegram notifications for your Slurm jobs.\n\n"
        "→ Use /link to get your API token\n"
        "→ Use /help to see all commands"
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "📋 *Commands*\n\n"
        "/start — Register\n"
        "/link — Generate API token\n"
        "/track <job\\_id> — Track a job\n"
        "/myjobs — List tracked jobs\n"
        "/stop <job\\_id> — Stop tracking\n"
        "/logs <job\\_id> — Last 20 lines of log\n\n"
        "📡 *API Usage*\n\n"
        "`register\\_job?job\\_id=X&token=Y`\n"
        "`notify?job\\_id=X&message=M&token=Y`",
        parse_mode="Markdown",
    )


async def myid(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(f"`{update.effective_user.id}`", parse_mode="Markdown")


async def link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    token = secrets.token_hex(16)
    with db() as cur:
        cur.execute(
            """
            INSERT INTO users (telegram_id, username, token)
            VALUES (%s, %s, %s)
            ON CONFLICT (telegram_id) DO UPDATE SET token = EXCLUDED.token
            """,
            (user.id, user.username or "N/A", token),
        )
    await update.message.reply_text(
        "🔑 *Your API token* (keep it secret):\n\n"
        f"`{token}`\n\n"
        "Use this in your Slurm scripts as `token=...`",
        parse_mode="Markdown",
    )


async def track(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Usage: /track <job_id>")
        return
    try:
        job_id = safe_job_id(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid job ID.")
        return

    tid = update.effective_user.id
    with db() as cur:
        cur.execute(
            "INSERT INTO jobs (job_id, telegram_id, status) VALUES (%s, %s, NULL) ON CONFLICT DO NOTHING",
            (job_id, tid),
        )
    await update.message.reply_text(f"✅ Tracking job `{job_id}`", parse_mode="Markdown")


async def myjobs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    tid = update.effective_user.id
    with db() as cur:
        cur.execute(
            "SELECT job_id, status FROM jobs WHERE telegram_id = %s ORDER BY ctid DESC",
            (tid,),
        )
        jobs = cur.fetchall()

    if not jobs:
        await update.message.reply_text("No tracked jobs.")
        return

    lines = [f"• `{r['job_id']}`  {r['status'] or '—'}" for r in jobs]
    await update.message.reply_text("📋 *Your jobs:*\n\n" + "\n".join(lines), parse_mode="Markdown")


async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Usage: /stop <job_id>")
        return
    try:
        job_id = safe_job_id(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid job ID.")
        return

    tid = update.effective_user.id
    with db() as cur:
        cur.execute(
            "DELETE FROM jobs WHERE job_id = %s AND telegram_id = %s",
            (job_id, tid),
        )
    await update.message.reply_text(f"🛑 Stopped tracking `{job_id}`", parse_mode="Markdown")


async def logs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Usage: /logs <job_id>")
        return
    try:
        job_id = safe_job_id(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid job ID.")
        return

    log_file = Path(CFG.log_dir) / f"slurm-{job_id}.out"
    try:
        log_file = log_file.resolve()
        log_file.relative_to(Path(CFG.log_dir).resolve())
    except ValueError:
        await update.message.reply_text("❌ Access denied.")
        return

    if not log_file.exists():
        await update.message.reply_text("Log file not found.")
        return

    try:
        output = subprocess.check_output(
            ["tail", "-n", "20", str(log_file)], timeout=5
        ).decode()
        await update.message.reply_text(
            f"```\n{output or '(empty)'}\n```", parse_mode="Markdown"
        )
    except subprocess.TimeoutExpired:
        await update.message.reply_text("⏱ Timed out reading log.")
    except Exception as exc:
        log.error("Error reading logs for %s: %s", job_id, exc)
        await update.message.reply_text("Error reading log file.")


# ── FastAPI ───────────────────────────────────────────────────────────────────
api = FastAPI(title="QueueWatch API")


@api.post("/register_job")
async def register_job(job_id: str, token: str) -> dict:
    """Register a job. token comes from /link in Telegram."""
    try:
        job_id = safe_job_id(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job_id")

    tid = _verify_token(token)
    with db() as cur:
        cur.execute(
            "INSERT INTO jobs (job_id, telegram_id, status) VALUES (%s, %s, NULL) ON CONFLICT DO NOTHING",
            (job_id, tid),
        )
    return {"status": "registered"}


@api.post("/notify")
async def notify(job_id: str, message: str, token: str) -> dict:
    """Send a notification. Only works for jobs belonging to this token's user."""
    tid = _verify_token(token)

    try:
        job_id = safe_job_id(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job_id")

    with db() as cur:
        cur.execute(
            "SELECT telegram_id FROM jobs WHERE job_id = %s AND telegram_id = %s",
            (job_id, tid),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Job not found or not yours")

        cur.execute("UPDATE jobs SET status = %s WHERE job_id = %s AND telegram_id = %s", (message, job_id, tid))
        cur.execute("INSERT INTO logs (job_id, message) VALUES (%s, %s)", (job_id, message))

    send_telegram(tid, f"[{job_id}] {message}")
    return {"status": "sent"}


@api.get("/health")
async def health() -> dict:
    with db() as cur:
        cur.execute("SELECT 1")
    return {"status": "ok"}


# ── Entry point ───────────────────────────────────────────────────────────────
BOT_COMMANDS = [
    BotCommand("start",  "Register"),
    BotCommand("help",   "Show help"),
    BotCommand("link",   "Generate API token"),
    BotCommand("track",  "Track a job"),
    BotCommand("myjobs", "List tracked jobs"),
    BotCommand("stop",   "Stop tracking a job"),
    BotCommand("logs",   "Show last 20 log lines"),
]


async def post_init(app) -> None:
    await app.bot.set_my_commands(BOT_COMMANDS)


def main() -> None:
    init_db()

    api_thread = threading.Thread(
        target=lambda: uvicorn.run(api, host=CFG.api_host, port=CFG.api_port),
        daemon=True,
    )
    api_thread.start()
    log.info("API listening on %s:%s", CFG.api_host, CFG.api_port)

    bot = (
        ApplicationBuilder()
        .token(CFG.bot_token)
        .request(HTTPXRequest())
        .post_init(post_init)
        .build()
    )

    for cmd, handler in [
        ("start",  start),
        ("help",   help_cmd),
        ("myid",   myid),
        ("link",   link),
        ("track",  track),
        ("myjobs", myjobs),
        ("stop",   stop),
        ("logs",   logs),
    ]:
        bot.add_handler(CommandHandler(cmd, handler))

    log.info("Bot polling…")
    bot.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()