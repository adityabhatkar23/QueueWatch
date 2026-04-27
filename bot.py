import sqlite3
import requests
import threading
import uvicorn
import secrets

from fastapi import FastAPI, HTTPException
from telegram import Update, BotCommand
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from telegram.request import HTTPXRequest

# ---------------- CONFIG ----------------
BOT_TOKEN = "8704067759:AAH5P9BuvRMIjr15_g_IYOE7EUEFNimTVGM"  # ⚠️ replace this

# ---------------- DB ----------------
conn = sqlite3.connect("db.sqlite", check_same_thread=False)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS users (
    telegram_id INTEGER PRIMARY KEY,
    username TEXT,
    token TEXT UNIQUE
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT,
    telegram_id INTEGER,
    status TEXT,
    UNIQUE(job_id, telegram_id)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS logs (
    job_id TEXT,
    message TEXT
)
""")

conn.commit()

# ---------------- HELPERS ----------------
def generate_token():
    return secrets.token_hex(8)

def send_telegram(tid, message):
    requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        data={"chat_id": tid, "text": message}
    )

# ---------------- BOT COMMANDS ----------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    username = user.username if user.username else "N/A"

    cur.execute("""
    INSERT INTO users (telegram_id, username)
    VALUES (?, ?)
    ON CONFLICT(telegram_id)
    DO UPDATE SET username=excluded.username
    """, (user.id, username))

    conn.commit()

    await update.message.reply_text(
        "Welcome to Slurm Notifier Bot\n\n"
        "Use /help to see commands"
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "/start - Register\n"
        "/link - Get token\n"
        "/track <job_id>\n"
        "/myjobs\n"
        "/stop <job_id>\n"
        "/status <job_id>\n"
        "/logs <job_id>\n"
    )


async def myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(str(update.effective_user.id))


async def link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    token = generate_token()

    cur.execute("""
    INSERT INTO users (telegram_id, username, token)
    VALUES (?, ?, ?)
    ON CONFLICT(telegram_id)
    DO UPDATE SET token=excluded.token
    """, (user.id, user.username or "N/A", token))

    conn.commit()

    await update.message.reply_text(f"Your token:\n`{token}`", parse_mode="Markdown")


async def track(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /track <job_id>")
        return

    job_id = context.args[0]
    tid = update.effective_user.id

    cur.execute("""
    INSERT OR IGNORE INTO jobs (job_id, telegram_id, status)
    VALUES (?, ?, ?)
    """, (job_id, tid, None))

    conn.commit()

    await update.message.reply_text(f"Tracking {job_id}")


async def myjobs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tid = update.effective_user.id

    cur.execute("SELECT job_id FROM jobs WHERE telegram_id=?", (tid,))
    jobs = cur.fetchall()

    if not jobs:
        await update.message.reply_text("No jobs")
        return

    await update.message.reply_text("\n".join([j[0] for j in jobs]))


async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /stop <job_id>")
        return

    job_id = context.args[0]
    tid = update.effective_user.id

    cur.execute(
        "DELETE FROM jobs WHERE job_id=? AND telegram_id=?",
        (job_id, tid)
    )
    conn.commit()

    await update.message.reply_text("Stopped")


import subprocess

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /status <job_id>")
        return

    job_id = context.args[0]

    try:
        result = subprocess.check_output(
            ["squeue", "-j", job_id, "-o", "%.18i %.9P %.8j %.8u %.2t %.10M %.6D %R"],
            stderr=subprocess.DEVNULL
        ).decode()

        if job_id not in result:
            raise Exception("Not in queue")

        await update.message.reply_text(f"{result}")

    except:
        # fallback to sacct (if available)
        try:
            result = subprocess.check_output(
                ["sacct", "-j", job_id, "--format=JobID,State,Elapsed"],
                stderr=subprocess.DEVNULL
            ).decode()

            await update.message.reply_text(result)

        except:
            await update.message.reply_text("Job not found or already finished")

import os

import os
import subprocess

async def logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /logs <job_id>")
        return

    job_id = context.args[0]

    log_file = f"slurm-{job_id}.out"

    if not os.path.exists(log_file):
        await update.message.reply_text("Log file not found")
        return

    try:
        output = subprocess.check_output(
            ["tail", "-n", "20", log_file]
        ).decode()

        await update.message.reply_text(output if output else "Log empty")

    except Exception:
        await update.message.reply_text("Error reading logs")

# ---------------- API ----------------

app_api = FastAPI()

@app_api.post("/register_job")
async def register_job(job_id: str, token: str):
    cur.execute("SELECT telegram_id FROM users WHERE token=?", (token,))
    user = cur.fetchone()

    if not user:
        raise HTTPException(status_code=404, detail="Invalid token")

    tid = user[0]

    cur.execute("""
    INSERT OR IGNORE INTO jobs (job_id, telegram_id, status)
    VALUES (?, ?, ?)
    """, (job_id, tid, None))

    conn.commit()

    return {"status": "registered"}


@app_api.post("/notify")
async def notify(job_id: str, message: str):
    cur.execute("SELECT telegram_id FROM jobs WHERE job_id=?", (job_id,))
    users = cur.fetchall()

    if not users:
        return {"status": "no users"}

    cur.execute("UPDATE jobs SET status=? WHERE job_id=?", (message, job_id))
    cur.execute("INSERT INTO logs VALUES (?, ?)", (job_id, message))
    conn.commit()

    for (tid,) in users:
        send_telegram(tid, message)

    return {"status": "sent"}


@app_api.get("/health")
async def health():
    return {"status": "ok"}


# ---------------- RUN ----------------

def run_api():
    uvicorn.run(app_api, host="0.0.0.0", port=8000)

request = HTTPXRequest()

bot_app = ApplicationBuilder().token(BOT_TOKEN).request(request).build()

bot_app.add_handler(CommandHandler("start", start))
bot_app.add_handler(CommandHandler("help", help_cmd))
bot_app.add_handler(CommandHandler("myid", myid))
bot_app.add_handler(CommandHandler("link", link))
bot_app.add_handler(CommandHandler("track", track))
bot_app.add_handler(CommandHandler("myjobs", myjobs))
bot_app.add_handler(CommandHandler("stop", stop))
bot_app.add_handler(CommandHandler("status", status))
bot_app.add_handler(CommandHandler("logs", logs))

async def set_commands(app):
    await app.bot.set_my_commands([
        BotCommand("start", "Register"),
        BotCommand("help", "Help"),
        BotCommand("link", "Get token"),
        BotCommand("track", "Track job"),
        BotCommand("myjobs", "List jobs"),
        BotCommand("stop", "Stop job"),
        BotCommand("status", "Job status"),
        BotCommand("logs", "Job logs"),
    ])

bot_app.post_init = set_commands

threading.Thread(target=run_api).start()

print("Running...")
bot_app.run_polling(drop_pending_updates=True)