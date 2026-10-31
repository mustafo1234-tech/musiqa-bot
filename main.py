"""
Telegram musiqa topuvchi bot.

Ishlatilgan texnologiyalar:
- aiogram 3.x
- yt-dlp (YouTube qidiruv va audio yuklash)
- PostgreSQL (asyncpg)
- python-dotenv

Ishga tushirish: python main.py
"""

from __future__ import annotations

import asyncio
import html
import logging
import os
import re
import shutil
import sys
import time
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

import asyncpg
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from dotenv import load_dotenv
from yt_dlp import YoutubeDL

# ---------------------------------------------------------------------------
# Asosiy sozlamalar
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
DOWNLOAD_DIR = BASE_DIR / "downloads"
FFMPEG_DIR = BASE_DIR / "ffmpeg" / "bin"

MAX_FILE_SIZE = 50 * 1024 * 1024  # Telegram botlar uchun 50 MB chegarasi
MAX_DURATION_SECONDS = 10 * 60  # 10 daqiqadan uzun ashulalar filtrlanadi
SEARCH_RESULTS_LIMIT = 10  # foydalanuvchiga ko'rsatiladigan maksimal natija
SEARCH_FETCH_LIMIT = 15  # uzun videolar filtrlanishini hisobga olib ko'proq qidiramiz
RESULTS_PER_PAGE = 5

SEARCH_COOLDOWN = 3.0  # qidiruvlar orasidagi minimal vaqt (soniya)
DOWNLOAD_COOLDOWN = 5.0  # yuklashlar orasidagi minimal vaqt (soniya)

# Windows konsolida emoji va boshqa Unicode belgilar log yozishni buzmasligi uchun.
for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8", errors="backslashreplace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("music_bot")


# ---------------------------------------------------------------------------
# Holatlar (FSM)
# ---------------------------------------------------------------------------


class SearchStates(StatesGroup):
    """Foydalanuvchi ashula nomini yozishini kutish holati."""

    waiting_for_query = State()


# ---------------------------------------------------------------------------
# Foydalanuvchiga ko'rsatiladigan xatoliklar
# ---------------------------------------------------------------------------


class UserFacingError(Exception):
    """Xabari foydalanuvchiga to'g'ridan-to'g'ri ko'rsatiladigan xatolik."""


class FileTooLargeError(UserFacingError):
    """Fayl hajmi 50 MB dan katta bo'lganda yuzaga keladi."""


class DurationTooLongError(UserFacingError):
    """Ashula davomiyligi 10 daqiqadan uzun bo'lganda yuzaga keladi."""


class FFmpegMissingError(UserFacingError):
    """ffmpeg topilmaganda yuzaga keladi."""


# ---------------------------------------------------------------------------
# Konfiguratsiya
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Config:
    bot_token: str
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str


def load_config() -> Config:
    """Sozlamalarni .env faylidan o'qiydi."""

    load_dotenv(BASE_DIR / ".env")

    # Eski .env fayllar bilan ham ishlashi uchun TOKEN zaxira varianti qoldirilgan.
    bot_token = (os.getenv("BOT_TOKEN") or os.getenv("TOKEN") or "").strip()
    if not bot_token:
        raise RuntimeError(
            "BOT_TOKEN topilmadi. .env faylini .env.example asosida to'ldiring."
        )

    raw_port = (os.getenv("DB_PORT") or "5432").strip()
    try:
        db_port = int(raw_port)
    except ValueError as exc:
        raise RuntimeError("DB_PORT butun son bo'lishi kerak.") from exc

    return Config(
        bot_token=bot_token,
        db_host=(os.getenv("DB_HOST") or "localhost").strip(),
        db_port=db_port,
        db_name=(os.getenv("DB_NAME") or "").strip(),
        db_user=(os.getenv("DB_USER") or "").strip(),
        db_password=os.getenv("DB_PASSWORD") or "",
    )


def find_ffmpeg() -> str | None:
    """PATH'dagi yoki loyiha ichidagi ffmpeg yo'lini qaytaradi."""

    system_ffmpeg = shutil.which("ffmpeg")
    if system_ffmpeg:
        return system_ffmpeg

    for name in ("ffmpeg.exe", "ffmpeg"):
        local_ffmpeg = FFMPEG_DIR / name
        if local_ffmpeg.is_file():
            return str(local_ffmpeg)

    return None


# ---------------------------------------------------------------------------
# PostgreSQL bilan ishlash
# ---------------------------------------------------------------------------


class Database:
    """asyncpg connection pool orqali bazaga yozuvlarni boshqaradi."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self.pool: asyncpg.Pool | None = None

    async def connect(self) -> bool:
        """Bazaga ulanadi va kerakli jadvallarni yaratadi."""

        if not self.config.db_name or not self.config.db_user:
            logger.error("DB_NAME yoki DB_USER ko'rsatilmagan. Bot bazasiz ishlaydi.")
            return False

        try:
            self.pool = await asyncpg.create_pool(
                host=self.config.db_host,
                port=self.config.db_port,
                database=self.config.db_name,
                user=self.config.db_user,
                password=self.config.db_password,
                min_size=1,
                max_size=10,
                timeout=10,
                command_timeout=30,
            )
            await self._create_tables()
            logger.info("PostgreSQL ga muvaffaqiyatli ulandi.")
            return True
        except Exception:
            logger.exception("PostgreSQL ga ulanishda xatolik yuz berdi.")
            if self.pool is not None:
                await self.pool.close()
                self.pool = None
            return False

    async def _create_tables(self) -> None:
        """Jadvallarni yaratadi va eski nomuvofiq sxemani moslashtiradi."""

        if self.pool is None:
            return

        async with self.pool.acquire() as connection:
            async with connection.transaction():
                # 1. Foydalanuvchilar jadvali.
                await connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        id BIGSERIAL PRIMARY KEY,
                        telegram_id BIGINT UNIQUE NOT NULL,
                        full_name TEXT,
                        username TEXT,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )

                # Eski bazada users.id bo'lmasligi mumkin — xavfsiz qo'shamiz.
                await connection.execute(
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS id BIGSERIAL"
                )
                await connection.execute(
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS full_name TEXT"
                )
                await connection.execute(
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT"
                )
                await connection.execute(
                    """
                    ALTER TABLE users
                    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    """
                )
                await connection.execute(
                    "UPDATE users SET created_at = NOW() WHERE created_at IS NULL"
                )
                await connection.execute(
                    "ALTER TABLE users ALTER COLUMN created_at SET DEFAULT NOW()"
                )
                await connection.execute(
                    "ALTER TABLE users ALTER COLUMN created_at SET NOT NULL"
                )

                await self._ensure_unique_index(
                    connection, "users", "id", "users_id_unique_idx"
                )
                await self._ensure_unique_index(
                    connection,
                    "users",
                    "telegram_id",
                    "users_telegram_id_unique_idx",
                )

                # 2. Qidiruvlar jadvali.
                await connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS searches (
                        id BIGSERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                        query TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                await connection.execute(
                    "ALTER TABLE searches ADD COLUMN IF NOT EXISTS user_id BIGINT"
                )
                await connection.execute(
                    "ALTER TABLE searches ADD COLUMN IF NOT EXISTS query TEXT"
                )
                await connection.execute(
                    """
                    ALTER TABLE searches
                    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    """
                )

                # 3. Yuklashlar jadvali.
                await connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS downloads (
                        id BIGSERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                        video_id TEXT NOT NULL,
                        title TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                await connection.execute(
                    "ALTER TABLE downloads ADD COLUMN IF NOT EXISTS user_id BIGINT"
                )
                await connection.execute(
                    "ALTER TABLE downloads ADD COLUMN IF NOT EXISTS video_id TEXT"
                )
                await connection.execute(
                    "ALTER TABLE downloads ADD COLUMN IF NOT EXISTS title TEXT"
                )
                await connection.execute(
                    """
                    ALTER TABLE downloads
                    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    """
                )

                # 4. Eski sxemada user_id users.telegram_id ga bog'langan bo'lishi
                #    mumkin. Uni users.id ga o'tkazamiz.
                await self._repair_user_foreign_key(connection, "searches")
                await self._repair_user_foreign_key(connection, "downloads")

    @staticmethod
    async def _ensure_unique_index(
        connection: asyncpg.Connection,
        table: str,
        column: str,
        index_name: str,
    ) -> None:
        """Ustunda yagona indeks borligini ta'minlaydi."""

        exists = await connection.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                FROM pg_index i
                JOIN pg_attribute a
                  ON a.attrelid = i.indrelid
                 AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = $1::regclass
                  AND i.indisunique
                  AND i.indnkeyatts = 1
                  AND a.attname = $2
            )
            """,
            table,
            column,
        )
        if not exists:
            await connection.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} "
                f"ON {table} ({column})"
            )

    @staticmethod
    async def _repair_user_foreign_key(
        connection: asyncpg.Connection, table: str
    ) -> None:
        """user_id tashqi kalitini users.id ga bog'laydi."""

        rows = await connection.fetch(
            """
            SELECT c.conname, pg_get_constraintdef(c.oid) AS definition
            FROM pg_constraint c
            WHERE c.conrelid = $1::regclass
              AND c.contype = 'f'
              AND c.confrelid = 'users'::regclass
              AND c.conkey = ARRAY(
                  SELECT a.attnum
                  FROM pg_attribute a
                  WHERE a.attrelid = c.conrelid AND a.attname = 'user_id'
              )
            """,
            table,
        )

        legacy_found = False
        valid_found = False

        for row in rows:
            definition = row["definition"] or ""
            if "telegram_id" in definition:
                legacy_found = True
                await connection.execute(
                    f'ALTER TABLE {table} DROP CONSTRAINT "{row["conname"]}"'
                )
            elif re.search(
                r"REFERENCES\s+users\s*\(\s*id\s*\)", definition, re.IGNORECASE
            ):
                valid_found = True

        if legacy_found:
            # Eski yozuvlardagi user_id (telegram_id) ni users.id ga almashtiramiz.
            await connection.execute(
                f"""
                UPDATE {table} AS target
                SET user_id = users.id
                FROM users
                WHERE target.user_id = users.telegram_id
                """
            )

        if not valid_found:
            await connection.execute(
                f"""
                ALTER TABLE {table}
                ADD CONSTRAINT {table}_user_id_fkey
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                """
            )

    async def add_user(
        self, telegram_id: int, full_name: str, username: str | None
    ) -> int | None:
        """Foydalanuvchini qo'shadi yoki mavjud ma'lumotlarini yangilaydi."""

        if self.pool is None:
            return None

        try:
            async with self.pool.acquire() as connection:
                row = await connection.fetchrow(
                    """
                    INSERT INTO users (telegram_id, full_name, username)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (telegram_id) DO UPDATE
                        SET full_name = EXCLUDED.full_name,
                            username = EXCLUDED.username
                    RETURNING id
                    """,
                    telegram_id,
                    full_name,
                    username,
                )
                return int(row["id"]) if row else None
        except Exception:
            logger.exception("Foydalanuvchini bazaga yozishda xatolik.")
            return None

    async def log_search(self, user_id: int, query: str) -> None:
        """Qidiruv tarixini saqlaydi."""

        if self.pool is None:
            return

        try:
            async with self.pool.acquire() as connection:
                await connection.execute(
                    "INSERT INTO searches (user_id, query) VALUES ($1, $2)",
                    user_id,
                    query,
                )
        except Exception:
            logger.exception("Qidiruvni bazaga yozishda xatolik.")

    async def log_download(self, user_id: int, video_id: str, title: str) -> None:
        """Yuklab olingan ashula tarixini saqlaydi."""

        if self.pool is None:
            return

        try:
            async with self.pool.acquire() as connection:
                await connection.execute(
                    """
                    INSERT INTO downloads (user_id, video_id, title)
                    VALUES ($1, $2, $3)
                    """,
                    user_id,
                    video_id or "",
                    title or "Noma'lum",
                )
        except Exception:
            logger.exception("Yuklashni bazaga yozishda xatolik.")

    async def close(self) -> None:
        """Connection pool ni yopadi."""

        if self.pool is not None:
            await self.pool.close()
            self.pool = None


# ---------------------------------------------------------------------------
# Throttling va bir vaqtda bajarilayotgan amallar
# ---------------------------------------------------------------------------

_last_action: dict[tuple[int, str], float] = {}
_busy_users: set[int] = set()


def is_throttled(user_id: int, action: str, cooldown: float) -> bool:
    """Foydalanuvchi amallari orasidagi minimal vaqtni tekshiradi."""

    now = time.monotonic()
    key = (user_id, action)
    last = _last_action.get(key, 0.0)
    if now - last < cooldown:
        return True
    _last_action[key] = now
    return False


# ---------------------------------------------------------------------------
# Yordamchi funksiyalar
# ---------------------------------------------------------------------------


def escape_html(value: Any) -> str:
    """HTML uchun xavfsiz matn qaytaradi."""

    return html.escape(str(value), quote=False)


def shorten(value: Any, limit: int) -> str:
    """Matnni kerakli uzunlikka qisqartiradi."""

    text = " ".join(str(value).split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def to_int(value: Any) -> int | None:
    """Qiymatni butun songa aylantiradi, imkonsiz bo'lsa None qaytaradi."""

    if value is None:
        return None
    try:
        number = int(float(value))
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def format_duration(seconds: int | None) -> str:
    """Davomiylikni daqiqa:soniya ko'rinishida chiqaradi."""

    if not seconds:
        return "—"
    minutes, secs = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


_INVALID_FILENAME_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


def sanitize_filename(name: str) -> str:
    """Windows uchun xavfli belgilarni fayl nomidan olib tashlaydi."""

    cleaned = _INVALID_FILENAME_CHARS.sub("_", name)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .")
    return cleaned[:120] or "audio"


def total_pages(results: list[dict[str, Any]]) -> int:
    """Natijalar uchun sahifalar sonini hisoblaydi."""

    return max(1, (len(results) + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE)


def build_results_text(
    query: str,
    results: list[dict[str, Any]],
    page: int,
    skipped_long: int = 0,
) -> str:
    """Qidiruv natijalarining chiroyli matnini tayyorlaydi."""

    pages = total_pages(results)
    page = max(0, min(page, pages - 1))
    start = page * RESULTS_PER_PAGE
    page_items = results[start : start + RESULTS_PER_PAGE]

    lines = [
        f"🎵 <b>Qidiruv:</b> <i>{escape_html(shorten(query, 80))}</i>",
        f"📄 Sahifa: <b>{page + 1}/{pages}</b>",
        "",
    ]

    for offset, item in enumerate(page_items):
        number = start + offset + 1
        lines.append(f"<b>{number}.</b> {escape_html(item['title'])}")
        lines.append(
            f"   👤 {escape_html(item['uploader'])}  |  "
            f"⏱ {format_duration(item['duration'])}"
        )

    if skipped_long:
        lines.append("")
        lines.append(
            f"ℹ️ 10 daqiqadan uzun {skipped_long} ta natija filtrlab tashlandi."
        )

    lines.append("")
    lines.append("👇 Ashulani tanlash uchun raqamli tugmani bosing.")
    return "\n".join(lines)


def build_results_keyboard(
    results: list[dict[str, Any]],
    page: int,
    search_id: str,
) -> InlineKeyboardMarkup:
    """Raqamli tugmalar va sahifalash tugmalarini yaratadi."""

    pages = total_pages(results)
    page = max(0, min(page, pages - 1))
    start = page * RESULTS_PER_PAGE
    page_items = results[start : start + RESULTS_PER_PAGE]

    rows: list[list[InlineKeyboardButton]] = []

    number_row = [
        InlineKeyboardButton(
            text=str(start + offset + 1),
            callback_data=f"pick:{search_id}:{start + offset}",
        )
        for offset in range(len(page_items))
    ]
    if number_row:
        rows.append(number_row)

    navigation_row: list[InlineKeyboardButton] = []
    if page > 0:
        navigation_row.append(
            InlineKeyboardButton(
                text="⬅️ Oldingi",
                callback_data=f"page:{search_id}:{page - 1}",
            )
        )
    if page < pages - 1:
        navigation_row.append(
            InlineKeyboardButton(
                text="Keyingi ➡️",
                callback_data=f"page:{search_id}:{page + 1}",
            )
        )
    if navigation_row:
        rows.append(navigation_row)

    return InlineKeyboardMarkup(inline_keyboard=rows)


async def edit_or_send(
    status_message: Message | None,
    text: str,
    fallback_message: Message | None = None,
) -> None:
    """Mavjud xabarni tahrirlaydi, imkonsiz bo'lsa yangi xabar yuboradi."""

    if status_message is not None:
        try:
            await status_message.edit_text(text)
            return
        except TelegramBadRequest:
            pass
        except Exception:
            logger.exception("Xabarni tahrirlashda kutilmagan xatolik.")

    if fallback_message is not None:
        try:
            await fallback_message.answer(text)
        except Exception:
            logger.exception("Xabar yuborishda xatolik.")


async def safe_delete(message: Message | None) -> None:
    """Xabarni xavfsiz o'chiradi."""

    if message is None:
        return
    try:
        await message.delete()
    except Exception:
        logger.debug("Xabarni o'chirib bo'lmadi.")


def download_error_message(error: Exception) -> str:
    """yt-dlp xatoligini foydalanuvchiga tushunarli matnga aylantiradi."""

    text = str(error).lower()

    if "ffmpeg" in text or "ffprobe" in text or "postprocessing" in text:
        return (
            "😔 MP3 ga aylantirishda xatolik yuz berdi. ffmpeg topilmadi yoki "
            "ishlamayapti. README.md dagi ffmpeg bo'limiga qarang."
        )
    if "private" in text or "sign in" in text or "log in" in text or "login" in text:
        return (
            "😔 Bu videoni yuklab bo'lmaydi (yopiq yoki cheklangan). "
            "Boshqa natijani tanlab ko'ring."
        )
    if "unavailable" in text or "not available" in text or "removed" in text:
        return "😔 Bu ashula mavjud emas yoki o'chirilgan. Boshqa natijani tanlab ko'ring."
    if (
        "timed out" in text
        or "timeout" in text
        or "connection" in text
        or "network" in text
        or "getaddrinfo" in text
        or "temporary failure" in text
    ):
        return (
            "🌐 Internet yoki YouTube bilan aloqa muammosi. "
            "Iltimos, keyinroq qayta urinib ko'ring."
        )
    return (
        "😔 Ashulani yuklab bo'lmadi. Boshqa natijani tanlab ko'ring "
        "yoki keyinroq qayta urinib ko'ring."
    )


# ---------------------------------------------------------------------------
# yt-dlp bilan ishlash (bloklovchi funksiyalar alohida threadda ishlaydi)
# ---------------------------------------------------------------------------


def search_youtube_sync(query: str) -> tuple[list[dict[str, Any]], int]:
    """YouTube'dan qidiruv natijalarini oladi."""

    options = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "noplaylist": True,
        "extract_flat": True,
        "socket_timeout": 30,
        "retries": 2,
        "geo_bypass": True,
        "noprogress": True,
    }

    with YoutubeDL(options) as ydl:
        data = ydl.extract_info(
            f"ytsearch{SEARCH_FETCH_LIMIT}:{query}", download=False
        )

    if not data:
        return [], 0

    results: list[dict[str, Any]] = []
    skipped_long = 0

    for entry in data.get("entries") or []:
        if not entry or not entry.get("id"):
            continue

        # Jonli translyatsiyalarni o'tkazib yuboramiz.
        if entry.get("is_live") or entry.get("live_status") in {"is_live", "is_upcoming"}:
            continue

        duration = to_int(entry.get("duration"))
        if duration is not None and duration > MAX_DURATION_SECONDS:
            skipped_long += 1
            continue

        video_id = str(entry["id"])
        results.append(
            {
                "video_id": video_id,
                "title": shorten(entry.get("title") or "Noma'lum ashula", 100),
                "uploader": shorten(
                    entry.get("uploader")
                    or entry.get("channel")
                    or "Noma'lum ijrochi",
                    80,
                ),
                "duration": duration,
                "url": entry.get("webpage_url")
                or f"https://www.youtube.com/watch?v={video_id}",
            }
        )

        if len(results) >= SEARCH_RESULTS_LIMIT:
            break

    return results, skipped_long


def download_audio_sync(url: str, user_id: int) -> tuple[str, dict[str, Any]]:
    """Audioni yuklab, mp3 ga aylantiradi va fayl yo'lini qaytaradi."""

    ffmpeg_path = find_ffmpeg()
    if ffmpeg_path is None:
        raise FFmpegMissingError(
            "😔 ffmpeg topilmadi. MP3 ga aylantirish uchun ffmpeg kerak. "
            "README.md dagi o'rnatish yo'riqnomasiga qarang."
        )

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    prefix = DOWNLOAD_DIR / f"{user_id}_{uuid4().hex}"
    output_template = f"{prefix}.%(ext)s"

    options = {
        "format": "bestaudio/best",
        "outtmpl": output_template,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "socket_timeout": 30,
        "retries": 2,
        "geo_bypass": True,
        "ffmpeg_location": ffmpeg_path,
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }
        ],
    }

    try:
        with YoutubeDL(options) as ydl:
            info = ydl.extract_info(url, download=True)

        if not info:
            raise RuntimeError("Audio ma'lumotlarini olishning iloji bo'lmadi.")

        downloaded_files = sorted(prefix.parent.glob(f"{prefix.name}.*"))
        mp3_path = next(
            (path for path in downloaded_files if path.suffix.lower() == ".mp3"),
            None,
        )

        if mp3_path is None:
            raise RuntimeError(
                "MP3 fayl yaratilmadi. ffmpeg va ffprobe to'g'ri "
                "ishlashini tekshiring."
            )

        return str(mp3_path), info
    except Exception:
        # Xatolik bo'lsa, yarim qolgan vaqtinchalik fayllarni tozalaymiz.
        for leftover in prefix.parent.glob(f"{prefix.name}.*"):
            try:
                leftover.unlink(missing_ok=True)
            except OSError:
                logger.warning("Vaqtinchalik faylni o'chirib bo'lmadi: %s", leftover)
        raise


# ---------------------------------------------------------------------------
# Dispatcher va handlerlar
# ---------------------------------------------------------------------------

dp = Dispatcher(storage=MemoryStorage())


@dp.message(CommandStart())
async def cmd_start(
    message: Message,
    state: FSMContext,
    db: Database,
    bot_name: str,
) -> None:
    """ /start buyrug'i: salomlashish va qidiruvni boshlash. """

    user = message.from_user
    if user is None:
        return

    await state.clear()
    await state.set_state(SearchStates.waiting_for_query)

    db_user_id = await db.add_user(user.id, user.full_name, user.username)
    if db_user_id is not None:
        await state.update_data(db_user_id=db_user_id)

    first_name = escape_html(user.first_name or "do'stim")
    bot_title = escape_html(bot_name or "Musiqa Bot")

    await message.answer(
        f"Salom, <b>{first_name}</b>! 👋\n\n"
        f"Men <b>{bot_title}</b> — musiqalarni topib beruvchi botman. 🎧\n"
        "YouTube'dan ashula qidirib, mp3 formatda yuboraman.\n\n"
        "Qaysi ashulani izlayapsiz? Ashula nomini yozing 🎵"
    )

    if db.pool is None:
        await message.answer(
            "⚠️ Ma'lumotlar bazasiga ulanib bo'lmadi. Bot ishlaydi, ammo "
            "qidiruv va yuklash tarixi saqlanmaydi."
        )


@dp.message(F.text)
async def handle_query(
    message: Message,
    state: FSMContext,
    db: Database,
) -> None:
    """Foydalanuvchi yozgan ashula nomini qabul qiladi va qidiradi."""

    user = message.from_user
    if user is None:
        return

    query = (message.text or "").strip()

    # Buyruqlar bu handlerga tushib qolsa, ularga javob bermaymiz.
    if query.startswith("/"):
        return

    if len(query) < 2:
        await message.answer(
            "✍️ Iltimos, ashula nomini to'liqroq yozing (kamida 2 ta belgi)."
        )
        return

    if len(query) > 200:
        await message.answer(
            "✍️ Ashula nomi juda uzun. Iltimos, 200 ta belgidan qisqaroq yozing."
        )
        return

    if user.id in _busy_users:
        await message.answer(
            "⏳ Hozir oldingi amal bajarilmoqda. Iltimos, biroz kuting..."
        )
        return

    if is_throttled(user.id, "search", SEARCH_COOLDOWN):
        await message.answer(
            "🐢 Biroz sekinroq! Iltimos, 3 soniyadan keyin qayta yuboring."
        )
        return

    _busy_users.add(user.id)
    status_message: Message | None = None

    try:
        status_message = await message.answer("🔎 Qidirilmoqda...")

        # Foydalanuvchi va qidiruvni darhol bazaga yozamiz.
        db_user_id = await db.add_user(user.id, user.full_name, user.username)
        if db_user_id is not None:
            await db.log_search(db_user_id, query)

        results, skipped_long = await asyncio.to_thread(search_youtube_sync, query)
        search_id = uuid4().hex[:10]

        await state.update_data(
            db_user_id=db_user_id,
            query=query,
            results=results,
            page=0,
            skipped_long=skipped_long,
            search_id=search_id,
        )

        if not results:
            if skipped_long:
                error_text = (
                    "😔 Faqat 10 daqiqadan uzun ashulalar topildi. "
                    "Iltimos, qisqaroq versiya yoki boshqa nom bilan urinib ko'ring."
                )
            else:
                error_text = (
                    "😔 Hech narsa topilmadi. Boshqacha nom bilan qayta urinib ko'ring."
                )
            await status_message.edit_text(error_text)
            return

        await state.set_state(SearchStates.waiting_for_query)
        await status_message.edit_text(
            build_results_text(query, results, 0, skipped_long),
            reply_markup=build_results_keyboard(results, 0, search_id),
            disable_web_page_preview=True,
        )

    except Exception:
        logger.exception(
            "Qidiruvda xatolik (user_id=%s, query=%r)", user.id, query
        )
        await edit_or_send(
            status_message,
            "🌐 Qidiruvda xatolik yuz berdi. Internet aloqasini tekshirib, "
            "qayta urinib ko'ring.",
            fallback_message=message,
        )
    finally:
        _busy_users.discard(user.id)


@dp.callback_query(F.data.startswith("page:"))
async def handle_page(callback: CallbackQuery, state: FSMContext) -> None:
    """Sahifalash tugmalari (oldingi/keyingi) uchun handler."""

    if callback.from_user.id in _busy_users:
        await callback.answer("⏳ Hozir amal bajarilmoqda. Iltimos, kuting...", show_alert=True)
        return

    if callback.message is None:
        await callback.answer("⚠️ Xabar topilmadi.", show_alert=True)
        return

    data = await state.get_data()
    results = data.get("results") or []
    query = data.get("query") or ""
    skipped_long = int(data.get("skipped_long") or 0)
    search_id = data.get("search_id") or ""

    if not results:
        await callback.answer(
            "⚠️ Natijalar eskirgan. Iltimos, qaytadan qidiring.",
            show_alert=True,
        )
        return

    try:
        _, callback_search_id, raw_page = (callback.data or "").split(":", 2)
        page = int(raw_page)
    except (IndexError, ValueError):
        await callback.answer("⚠️ Noto'g'ri tugma.", show_alert=True)
        return

    if callback_search_id != search_id:
        await callback.answer(
            "⚠️ Bu eski natijalar. Iltimos, eng oxirgi xabardagi tugmalardan foydalaning.",
            show_alert=True,
        )
        return

    page = max(0, min(page, total_pages(results) - 1))
    await state.update_data(page=page)
    await callback.answer()

    try:
        await callback.message.edit_text(
            build_results_text(query, results, page, skipped_long),
            reply_markup=build_results_keyboard(results, page, search_id),
            disable_web_page_preview=True,
        )
    except TelegramBadRequest:
        logger.debug("Sahifani tahrirlashda TelegramBadRequest yuz berdi.")


@dp.callback_query(F.data.startswith("pick:"))
async def handle_pick(
    callback: CallbackQuery,
    state: FSMContext,
    db: Database,
) -> None:
    """Tanlangan ashulani yuklab, audio sifatida yuboradi."""

    user = callback.from_user
    if callback.message is None:
        await callback.answer("⚠️ Xabar topilmadi.", show_alert=True)
        return

    if user.id in _busy_users:
        await callback.answer(
            "⏳ Hozir oldingi amal bajarilmoqda. Iltimos, kuting...",
            show_alert=True,
        )
        return

    data = await state.get_data()
    results = data.get("results") or []
    search_id = data.get("search_id") or ""

    try:
        _, callback_search_id, raw_index = (callback.data or "").split(":", 2)
        index = int(raw_index)
    except (IndexError, ValueError):
        await callback.answer("⚠️ Noto'g'ri tanlov.", show_alert=True)
        return

    if callback_search_id != search_id:
        await callback.answer(
            "⚠️ Bu eski natijalar. Iltimos, eng oxirgi xabardagi tugmalardan foydalaning.",
            show_alert=True,
        )
        return

    if not (0 <= index < len(results)):
        await callback.answer(
            "⚠️ Natijalar eskirgan. Iltimos, qaytadan qidiring.",
            show_alert=True,
        )
        return

    if is_throttled(user.id, "download", DOWNLOAD_COOLDOWN):
        await callback.answer(
            "🐢 Biroz sekinroq! 5 soniyadan keyin qayta urinib ko'ring.",
            show_alert=True,
        )
        return

    await callback.answer()

    item = results[index]
    _busy_users.add(user.id)
    status_message: Message | None = None
    temporary_path: Path | None = None

    try:
        status_message = await callback.message.answer(
            f"⏳ Yuklanmoqda: <b>{escape_html(item['title'])}</b>\n"
            "Iltimos, biroz kuting..."
        )

        audio_path, info = await asyncio.to_thread(
            download_audio_sync, item["url"], user.id
        )
        temporary_path = Path(audio_path)

        file_size = temporary_path.stat().st_size
        if file_size > MAX_FILE_SIZE:
            raise FileTooLargeError(
                "😔 Afsus, bu ashula 50 MB dan katta. Telegram botlar 50 MB gacha "
                "fayl yubora oladi. Iltimos, boshqa, qisqaroq versiyani tanlang."
            )

        duration = to_int(info.get("duration")) or item.get("duration")
        if duration is not None and duration > MAX_DURATION_SECONDS:
            raise DurationTooLongError(
                "😔 Ashula 10 daqiqadan uzun. Iltimos, qisqaroq versiyani tanlang."
            )

        title = shorten(info.get("title") or item["title"], 64)
        performer = shorten(
            info.get("uploader")
            or info.get("channel")
            or item["uploader"],
            64,
        )
        filename = sanitize_filename(f"{performer} - {title}") + ".mp3"
        caption = (
            f"🎵 <b>{escape_html(shorten(title, 120))}</b>\n"
            f"👤 {escape_html(shorten(performer, 120))}\n"
            f"⏱ {format_duration(duration)}"
        )

        await callback.message.answer_audio(
            audio=FSInputFile(temporary_path, filename=filename),
            title=title,
            performer=performer,
            duration=duration if duration else None,
            caption=caption,
        )

        # Yuklashni bazaga to'liq nom bilan yozamiz.
        db_user_id = data.get("db_user_id")
        if db_user_id is None:
            db_user_id = await db.add_user(user.id, user.full_name, user.username)
        if db_user_id is not None:
            full_title = str(info.get("title") or item["title"])
            await db.log_download(
                db_user_id, item.get("video_id", ""), full_title
            )

        await safe_delete(status_message)
        await callback.message.answer(
            "Yana qaysi ashulani izlaymiz? Nomini yozing 🎶"
        )

    except UserFacingError as error:
        logger.warning("Foydalanuvchiga ko'rsatiladigan xatolik: %s", error)
        await edit_or_send(
            status_message, str(error), fallback_message=callback.message
        )
    except Exception as error:
        logger.exception(
            "Audio yuklashda xatolik (user_id=%s, video_id=%s)",
            user.id,
            item.get("video_id"),
        )
        await edit_or_send(
            status_message,
            download_error_message(error),
            fallback_message=callback.message,
        )
    finally:
        _busy_users.discard(user.id)
        if temporary_path is not None:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                logger.warning(
                    "Vaqtinchalik faylni o'chirib bo'lmadi: %s", temporary_path
                )


# ---------------------------------------------------------------------------
# Botni ishga tushirish
# ---------------------------------------------------------------------------


async def start_health_server() -> None:
    """Render kabi platformalar uchun $PORT da oddiy HTTP health server ochadi."""

    port = int(os.getenv("PORT", "8080"))

    async def health(_: web.Request) -> web.Response:
        return web.Response(text="OK")

    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info("Health server %s-portda ishga tushdi.", port)


async def main() -> None:
    """Botni ishga tushiradi."""

    try:
        config = load_config()
    except RuntimeError as error:
        logger.error("%s", error)
        return

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    # Oldingi ishga tushirishdan qolgan vaqtinchalik fayllarni tozalaymiz.
    for old_file in DOWNLOAD_DIR.glob("*"):
        if old_file.is_file():
            try:
                old_file.unlink(missing_ok=True)
            except OSError:
                logger.warning("Eski vaqtinchalik faylni o'chirib bo'lmadi: %s", old_file)

    ffmpeg_path = find_ffmpeg()
    if ffmpeg_path is None:
        logger.warning(
            "ffmpeg topilmadi! MP3 ga aylantirish ishlamaydi. "
            "README.md dagi ffmpeg bo'limiga qarang."
        )
    else:
        logger.info("ffmpeg topildi: %s", ffmpeg_path)

    database = Database(config)
    database_connected = await database.connect()
    if not database_connected:
        logger.warning(
            "Ma'lumotlar bazasiga ulanib bo'lmadi. Bot bazasiz ishlashda davom etadi."
        )

    bot = Bot(
        token=config.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    health_task = asyncio.create_task(start_health_server())

    try:
        me = await bot.get_me()
        bot_name = me.first_name or me.username or "Musiqa Bot"

        try:
            await bot.set_my_commands(
                [BotCommand(command="start", description="🎵 Botni boshlash")]
            )
        except Exception:
            logger.exception("Bot buyruqlarini o'rnatib bo'lmadi.")

        logger.info("Bot ishga tushdi: @%s", me.username)
        logger.info("To'xtatish uchun Ctrl+C bosing.")

        await dp.start_polling(
            bot,
            db=database,
            bot_name=bot_name,
            allowed_updates=dp.resolve_used_update_types(),
            close_bot_session=False,
        )
    except Exception:
        logger.exception("Bot ishga tushishida yoki ishlashida xatolik.")
    finally:
        health_task.cancel()
        with suppress(asyncio.CancelledError):
            await health_task
        await database.close()
        await bot.session.close()
        logger.info("Bot to'xtatildi.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot foydalanuvchi tomonidan to'xtatildi.")
