import asyncio
import logging
from contextlib import contextmanager
from typing import Dict, Any, Optional, List

import pymysql
from pymysql.cursors import DictCursor

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramForbiddenError
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BotCommand,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


class AdminStates(StatesGroup):
    waiting_for_numbers = State()
    waiting_for_file = State()
    waiting_for_forward = State()
    waiting_for_remove_forward = State()


class UserStates(StatesGroup):
    waiting_for_agent_name = State()
    waiting_for_summary = State()
    waiting_for_callback_summary = State()
    waiting_for_finishing_summary = State()
    waiting_for_call_ended_summary = State()


class LineBot:
    """
    One reusable bot instance.

    Per-bot config fields:
      - bot_token
      - admin_id
      - group_chat_id
      - reference
      - database: {host, user, password, database, charset}
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config

        self.bot_token: str = config["bot_token"]
        self.admin_id: int = config["admin_id"]
        self.group_chat_id: int = config["group_chat_id"]
        self.reference: str = config.get("reference", "CB2061")

        db_cfg = config["database"]
        self.db_config = {
            "host": db_cfg["host"],
            "user": db_cfg["user"],
            "password": db_cfg["password"],
            "database": db_cfg["database"],
            "charset": db_cfg.get("charset", "utf8mb4"),
            "cursorclass": DictCursor,
        }

        self.bot: Optional[Bot] = None
        self.dp: Optional[Dispatcher] = None
        self.router: Router = Router()

        self._register_handlers()
        logging.info(f"✅ LineBot created for reference {self.reference}")

    # ============= DB CONNECTION =============

    @contextmanager
    def get_db_connection(self):
        conn = pymysql.connect(**self.db_config)
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.error(f"Database error: {e}")
            raise
        finally:
            conn.close()

    # ============= DB INIT =============

    def init_database(self):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bot_status (
                       id INT PRIMARY KEY DEFAULT 1,
                       status VARCHAR(50) DEFAULT 'running'
                    )
                    """
                )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS no_answer_records (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        username VARCHAR(255),
                        agent_name VARCHAR(255),
                        reference VARCHAR(50),
                        number VARCHAR(255),
                        name VARCHAR(255),
                        address TEXT,
                        email VARCHAR(255),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_user_id (user_id),
                        INDEX idx_created_at (created_at)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )

                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS users (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        user_id BIGINT NOT NULL UNIQUE,
                        agent_name VARCHAR(255),
                        reference VARCHAR(50) DEFAULT '{self.reference}',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_user_id (user_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS number_queue (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        number VARCHAR(255) NOT NULL,
                        name VARCHAR(255) DEFAULT NULL,
                        address TEXT DEFAULT NULL,
                        email VARCHAR(255) DEFAULT NULL,
                        is_used BOOLEAN DEFAULT FALSE,
                        is_completed BOOLEAN DEFAULT FALSE,
                        used_by_user_id BIGINT DEFAULT NULL,
                        used_by_username VARCHAR(255) DEFAULT NULL,
                        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        used_at TIMESTAMP NULL DEFAULT NULL,
                        status VARCHAR(50) DEFAULT NULL,
                        call_summary TEXT DEFAULT NULL,
                        summary_submitted_at TIMESTAMP NULL DEFAULT NULL,
                        group_chat_id BIGINT DEFAULT NULL,
                        INDEX idx_is_used (is_used),
                        INDEX idx_is_completed (is_completed),
                        INDEX idx_added_at (added_at),
                        INDEX idx_user_id (used_by_user_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS number_requests (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        username VARCHAR(255) DEFAULT NULL,
                        group_chat_id BIGINT NOT NULL,
                        previous_number VARCHAR(255) DEFAULT NULL,
                        reason VARCHAR(100) DEFAULT NULL,
                        status VARCHAR(50) DEFAULT 'pending',
                        requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        processed_at TIMESTAMP NULL DEFAULT NULL,
                        INDEX idx_status (status),
                        INDEX idx_user_id (user_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS admins (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        user_id BIGINT NOT NULL UNIQUE,
                        username VARCHAR(255),
                        added_by BIGINT NOT NULL,
                        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_user_id (user_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )

                logging.info(
                    f"✅ Database tables initialized for reference {self.reference}"
                )

    # ============= DB HELPERS =============

    def save_agent_name(self, user_id: int, agent_name: str):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO users (user_id, agent_name, reference)
                    VALUES (%s, %s, '{self.reference}')
                    ON DUPLICATE KEY UPDATE agent_name = %s
                    """,
                    (user_id, agent_name, agent_name),
                )

    def get_user_info(self, user_id: int) -> Optional[Dict[str, Any]]:
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT agent_name, reference FROM users WHERE user_id = %s",
                    (user_id,),
                )
                return cursor.fetchone()

    def is_admin(self, user_id: int) -> bool:
        if user_id == self.admin_id:
            return True
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id FROM admins WHERE user_id = %s", (user_id,))
                return cursor.fetchone() is not None

    def get_all_admins(self) -> List[Dict[str, Any]]:
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT user_id, username, added_at FROM admins ORDER BY added_at"
                )
                return cursor.fetchall()

    def get_user_record(self, user_id: int) -> Optional[Dict[str, Any]]:
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT * FROM number_queue
                    WHERE used_by_user_id = %s
                      AND is_used = TRUE
                      AND is_completed = FALSE
                    ORDER BY used_at DESC, id DESC
                    LIMIT 1
                    """,
                    (user_id,),
                )
                row = cursor.fetchone()
                return row if row else None

    def mark_line_completed(self, user_id: int):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE number_queue SET is_completed = TRUE "
                    "WHERE used_by_user_id = %s AND is_used = TRUE",
                    (user_id,),
                )
                cursor.execute(
                    "DELETE FROM number_requests "
                    "WHERE user_id = %s AND status IN ('pending','approved')",
                    (user_id,),
                )

    def get_next_number(
        self, user_id: int, username: Optional[str] = None, force_new: bool = False
    ) -> Optional[Dict[str, Any]]:
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                if not force_new:
                    cursor.execute(
                        """
                        SELECT number FROM number_queue
                        WHERE used_by_user_id = %s
                          AND is_used = TRUE
                          AND is_completed = FALSE
                        LIMIT 1
                        """,
                        (user_id,),
                    )
                    existing = cursor.fetchone()
                    if existing:
                        return None

                cursor.execute(
                    """
                    SELECT id, number, name, address, email
                    FROM number_queue
                    WHERE is_used = FALSE AND is_completed = FALSE
                    ORDER BY id
                    LIMIT 1
                    """
                )
                result = cursor.fetchone()
                if not result:
                    return None

                record_id = result["id"]
                cursor.execute(
                    """
                    UPDATE number_queue
                    SET is_used = TRUE,
                        used_by_user_id = %s,
                        used_by_username = %s,
                        used_at = NOW()
                    WHERE id = %s
                    """,
                    (user_id, username, record_id),
                )
                return result

    def update_record_status(self, user_id: int, status: str):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE number_queue SET status = %s WHERE used_by_user_id = %s",
                    (status, user_id),
                )

    def save_no_answer_record(
        self,
        user_id: int,
        username: str,
        agent_name: str,
        reference: str,
        number: str,
        name: Optional[str],
        address: Optional[str],
        email: Optional[str],
    ):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO no_answer_records
                      (user_id, username, agent_name, reference, number, name, address, email)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        user_id,
                        username,
                        agent_name,
                        reference,
                        number,
                        name,
                        address,
                        email,
                    ),
                )

    def set_bot_status(self, status: str):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO bot_status (id, status)
                    VALUES (1, %s)
                    ON DUPLICATE KEY UPDATE status = %s
                    """,
                    (status, status),
                )

    def get_bot_status(self) -> str:
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT status FROM bot_status WHERE id = 1")
                row = cursor.fetchone()
                return row["status"] if row else "running"

    def reset_queue(self) -> int:
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE number_queue
                       SET is_used = FALSE,
                           used_by_user_id = NULL,
                           used_by_username = NULL,
                           used_at = NULL,
                           status = NULL,
                           call_summary = NULL,
                           summary_submitted_at = NULL
                     WHERE is_completed = FALSE
                    """
                )
                return cursor.rowcount

    # ============= MORE DB HELPERS =============

    def get_queue_stats(self):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) as count FROM number_queue WHERE is_used = FALSE"
                )
                remaining = cursor.fetchone()["count"]

                cursor.execute(
                    "SELECT COUNT(*) as count FROM number_queue WHERE is_used = TRUE"
                )
                used = cursor.fetchone()["count"]

                return remaining, used

    def clear_pending_requests(self) -> int:
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM number_requests WHERE status IN ('pending','approved')"
                )
                return cursor.rowcount

    def clear_queue(self) -> int:
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM number_queue")
                cursor.execute("DELETE FROM number_requests")
                return cursor.rowcount

    def add_numbers_to_queue(self, numbers_list: List[str]):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                query = "INSERT INTO number_queue (number) VALUES (%s)"
                cursor.executemany(query, [(num.strip(),) for num in numbers_list])

    def add_records_from_csv(self, records_list: List[tuple]):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                query = """INSERT INTO number_queue (number, name, address, email) 
                           VALUES (%s, %s, %s, %s)"""
                cursor.executemany(query, records_list)

    def export_used_numbers(self):
        import io
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """SELECT number, name, address, email, used_by_username, used_by_user_id, 
                       status, call_summary, used_at, summary_submitted_at 
                       FROM number_queue 
                       WHERE is_completed = TRUE
                       ORDER BY used_at"""
                )
                results = cursor.fetchall()

                if not results:
                    return None, 0

                output = io.StringIO()
                output.write("Number,Name,Address,Email,Used By,User ID,Status,Summary,Used At,Summary At\n")
                for row in results:
                    status = row["status"] if row["status"] else "-"
                    summary = row["call_summary"].replace('"', '""') if row["call_summary"] else "-"
                    summary_at = row["summary_submitted_at"] if row["summary_submitted_at"] else "-"
                    name = row["name"] if row["name"] else "-"
                    address = row["address"] if row["address"] else "-"
                    email = row["email"] if row["email"] else "-"
                    output.write(
                        f'{row["number"]},{name},"{address}",{email},{row["used_by_username"]},{row["used_by_user_id"]},{status},"{summary}",{row["used_at"]},{summary_at}\n'
                    )

                cursor.execute("DELETE FROM number_queue WHERE is_completed = TRUE")
                deleted_count = cursor.rowcount

                return output.getvalue(), deleted_count

    def export_unused_numbers(self):
        import io
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """SELECT number, name, address, email 
                       FROM number_queue 
                       WHERE is_used = FALSE AND is_completed = FALSE
                       ORDER BY id"""
                )
                results = cursor.fetchall()

                if not results:
                    return None, 0

                output = io.StringIO()
                output.write("Number,Name,Address,Email\n")
                for row in results:
                    name = row["name"] if row["name"] else "-"
                    address = row["address"] if row["address"] else "-"
                    email = row["email"] if row["email"] else "-"
                    output.write(f'{row["number"]},{name},"{address}",{email}\n')

                return output.getvalue(), 0

    def export_all_numbers(self):
        import io
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """SELECT number, name, address, email, is_used, used_by_username, 
                       used_by_user_id, status, call_summary, used_at, added_at 
                       FROM number_queue 
                       ORDER BY id"""
                )
                results = cursor.fetchall()

                if not results:
                    return None, 0

                output = io.StringIO()
                output.write("Number,Name,Address,Email,Used,Username,User ID,Status,Summary,Used At,Added At\n")
                for row in results:
                    used = "Yes" if row["is_used"] else "No"
                    name = row["name"] if row["name"] else "-"
                    address = row["address"] if row["address"] else "-"
                    email = row["email"] if row["email"] else "-"
                    username = row["used_by_username"] if row["used_by_username"] else "-"
                    user_id = row["used_by_user_id"] if row["used_by_user_id"] else "-"
                    status = row["status"] if row["status"] else "-"
                    summary = row["call_summary"].replace('"', '""') if row["call_summary"] else "-"
                    used_at = row["used_at"] if row["used_at"] else "-"
                    output.write(
                        f'{row["number"]},{name},"{address}",{email},{used},{username},{user_id},{status},"{summary}",{used_at},{row["added_at"]}\n'
                    )

                cursor.execute("DELETE FROM number_queue")
                deleted_count = cursor.rowcount

                return output.getvalue(), deleted_count

    def export_no_answer_records(self):
        import io
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """SELECT user_id, username, agent_name, reference, number, name, 
                       address, email, created_at 
                       FROM no_answer_records 
                       ORDER BY created_at DESC"""
                )
                results = cursor.fetchall()

                if not results:
                    return None, 0

                output = io.StringIO()
                output.write("User ID,Username,Agent Name,Reference,Number,Name,Address,Email,Date\n")
                for row in results:
                    name = row["name"] if row["name"] else "-"
                    address = row["address"].replace('"', '""') if row["address"] else "-"
                    email = row["email"] if row["email"] else "-"
                    output.write(
                        f'{row["user_id"]},{row["username"]},{row["agent_name"]},{row["reference"]},{row["number"]},{name},"{address}",{email},{row["created_at"]}\n'
                    )

                cursor.execute("DELETE FROM no_answer_records")
                deleted_count = cursor.rowcount

                return output.getvalue(), deleted_count

    def add_admin(self, user_id: int, username: str, added_by: int):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO admins (user_id, username, added_by) VALUES (%s, %s, %s)",
                    (user_id, username, added_by),
                )

    def remove_admin(self, user_id: int) -> int:
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM admins WHERE user_id = %s", (user_id,))
                return cursor.rowcount

    def update_record_summary(self, user_id: int, summary: str):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """UPDATE number_queue 
                       SET call_summary = %s, summary_submitted_at = NOW() 
                       WHERE used_by_user_id = %s""",
                    (summary, user_id),
                )


    # ============= COMMANDS SETUP =============

    async def set_bot_commands(self):
        from aiogram.types import BotCommandScopeChat

        admin_commands = [
            BotCommand(command="start", description="Start the bot"),
            BotCommand(command="line", description="Request a new line"),
            BotCommand(command="stats", description="View queue statistics"),
            BotCommand(command="reset", description="Reset all numbers"),
            BotCommand(command="clear", description="Delete all numbers"),
            BotCommand(command="clearrequests", description="Clear pending requests"),
            BotCommand(command="export_used", description="Export used numbers"),
            BotCommand(command="export_unused", description="Export unused numbers"),
            BotCommand(command="export_all", description="Export all data"),
            BotCommand(command="export_no_answer", description="Export no answer records"),
            BotCommand(command="stop", description="Stop the bot"),
        ]

        user_commands = [
            BotCommand(command="start", description="Start the bot"),
        ]

        await self.bot.set_my_commands(
            admin_commands, scope=BotCommandScopeChat(chat_id=self.admin_id)
        )

        try:
            admins = self.get_all_admins()
            for adm in admins:
                if adm["user_id"] != self.admin_id:
                    try:
                        await self.bot.set_my_commands(
                            admin_commands,
                            scope=BotCommandScopeChat(chat_id=adm["user_id"]),
                        )
                    except Exception as e:
                        logging.error(
                            f"Failed to set commands for admin {adm['user_id']}: {e}"
                        )
        except Exception as e:
            logging.error(f"Error getting admins: {e}")

        await self.bot.set_my_commands(user_commands)
        logging.info(f"✅ Commands set for reference {self.reference}")

    # ============= HANDLERS =============

    def _register_handlers(self):
        r = self.router

        @r.message(Command("start"))
        async def cmd_start(message: Message, state: FSMContext):
            if message.chat.type != "private":
                await message.answer("👋 Use /start in private chat!")
                return

            if message.from_user.id == self.admin_id:
                status = self.get_bot_status()
                if status == "stopped":
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, self.set_bot_status, "running")
                    await message.answer(
                        "▶️ <b>Bot restarted!</b>\n\nUsers can now request lines.",
                        parse_mode="HTML",
                    )
                else:
                    await message.answer(
                        f"🤖 <b>Admin Panel ({self.reference})</b>\n\n"
                        "/stats - View queue statistics\n"
                        "/reset - Reset all numbers\n"
                        "/clear - Delete all numbers\n"
                        "/export_used - Export used numbers\n"
                        "/export_unused - Export remaining\n"
                        "/export_all - Export everything",
                        parse_mode="HTML",
                    )
                return

            try:
                member = await message.bot.get_chat_member(
                    self.group_chat_id, message.from_user.id
                )
                if member.status == "kicked":
                    await message.answer("⚠️ You're blocked from the group!")
                    return
                if member.status == "left":
                    await message.answer("⚠️ You must be a member of the group!")
                    return
            except Exception:
                await message.answer("⚠️ You must be a member of the group!")
                return

            loop = asyncio.get_event_loop()
            user_info = await loop.run_in_executor(
                None, self.get_user_info, message.from_user.id
            )

            if user_info and user_info.get("agent_name"):
                kb = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="Line 📞", callback_data="request_line")]
                    ]
                )
                await message.answer(
                    f"👋 Welcome back, <b>{user_info['agent_name']}</b>!\n\n"
                    f"🗃️ Reference: <code>{user_info['reference']}</code>\n\n"
                    "Press the button below to request a line",
                    parse_mode="HTML",
                    reply_markup=kb,
                )
            else:
                await state.set_state(UserStates.waiting_for_agent_name)
                await message.answer(
                    "👋 Welcome! Please enter your agent name:\n\nSend /cancel to abort.",
                    parse_mode="HTML",
                )

        @r.message(UserStates.waiting_for_agent_name, F.text)
        async def receive_agent_name(message: Message, state: FSMContext):
            if message.text.startswith("/cancel"):
                await state.clear()
                await message.answer("❌ Cancelled.")
                return

            agent_name = message.text.strip()
            if not (2 <= len(agent_name) <= 50):
                await message.answer("❌ Agent name must be 2-50 characters!")
                return

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None, self.save_agent_name, message.from_user.id, agent_name
            )

            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="Line 📞", callback_data="request_line")]
                ]
            )

            await message.answer(
                f"✅ <b>Agent name saved!</b>\n\n"
                f"👤 <b>Agent:</b> {agent_name}\n"
                f"🗃️ Reference: <code>{self.reference}</code>\n\n"
                "Press the button below to request a line",
                parse_mode="HTML",
                reply_markup=kb,
            )
            await state.clear()

        @r.callback_query(F.data == "request_line")
        async def callback_request_line(callback: CallbackQuery, state: FSMContext):
            user_id = callback.from_user.id
            username = callback.from_user.username or callback.from_user.first_name

            # group membership check (keep as-is)
            try:
                member = await self.bot.get_chat_member(self.group_chat_id, user_id)
                if member.status == "kicked":
                    await callback.message.answer("⚠️ You're blocked from the group!")
                    await callback.answer()
                    return
                if member.status == "left":
                    await callback.message.answer("⚠️ Join the group first!")
                    await callback.answer()
                    return
            except Exception:
                await callback.message.answer("⚠️ Join the group first!")
                await callback.answer()
                return

            # bot running check (keep)
            status = self.get_bot_status()
            if status == "stopped":
                await callback.message.answer("⏸️ Bot is currently stopped.", parse_mode="HTML")
                await callback.answer()
                return

            loop = asyncio.get_event_loop()

            # still prevent multiple active lines for same user
            def check_has_active_line(uid: int):
                with self.get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            """
                            SELECT id FROM number_queue
                            WHERE used_by_user_id = %s
                              AND is_used = TRUE
                              AND is_completed = FALSE
                            LIMIT 1
                            """,
                            (uid,),
                        )
                        return cursor.fetchone() is not None

            has_line = await loop.run_in_executor(None, check_has_active_line, user_id)
            if has_line:
                await callback.message.answer("❌ You already have an active line! Use it first.")
                await callback.answer()
                return

            # DIRECTLY assign next number (no number_requests, no admin approval)
            record = await loop.run_in_executor(
                None, self.get_next_number, user_id, username
            )

            if not record:
                await callback.message.answer("❌ No lines available right now.")
                await callback.answer()
                return

            # fetch agent info
            user_info = await loop.run_in_executor(
                None, self.get_user_info, user_id
            )
            agent_name = (
                user_info["agent_name"]
                if user_info and user_info.get("agent_name")
                else "Agent"
            )
            reference = user_info["reference"] if user_info else self.reference

            # build DM text similar to approve_line success
            dm = ""
            dm += f"👤 <b>Agent:</b> {agent_name}\n"
            dm += f"🗃️ Reference: <code>{reference}</code>\n\n"
            dm += "🎫 <b>Your Line:</b>\n\n"
            if record.get("name"):
                dm += f"👤 Name: {record['name']}\n"
            if record.get("address"):
                dm += f"📍 Address: {record['address']}\n"
            if record.get("email"):
                dm += f"✉️ Email: {record['email']}\n"
            dm += f"📞 Number: <code>{record['number']}</code>\n\n"
            dm += "✅ Please call this number now and then submit the call result."

            try:
                await self.bot.send_message(
                    chat_id=user_id, text=dm, parse_mode="HTML"
                )
                await callback.message.answer("✅ Line assigned! Check your DM.")
            except TelegramForbiddenError:
                await callback.message.answer(
                    "⚠️ Please start the bot in private first, then click /start again."
                )

            await callback.answer()

        @r.callback_query(F.data.startswith("approve_line_"))
        async def callback_approve_line(callback: CallbackQuery):
            loop = asyncio.get_event_loop()

            if not await loop.run_in_executor(
                None, self.is_admin, callback.from_user.id
            ):
                await callback.answer("❌ Admin only!", show_alert=True)
                return

            request_id = int(callback.data.split("_")[2])

            def get_request_by_id(req_id: int):
                with self.get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            "SELECT * FROM number_requests WHERE id = %s", (req_id,)
                        )
                        return cursor.fetchone()

            request = await loop.run_in_executor(None, get_request_by_id, request_id)
            if not request:
                await callback.answer("❌ Not found or already processed!", show_alert=True)
                try:
                    await callback.message.delete()
                except Exception:
                    pass
                return

            if request["status"] != "pending":
                await callback.answer(
                    f"⚠️ Already {request['status']}!", show_alert=True
                )
                try:
                    await callback.message.delete()
                except Exception:
                    pass
                return

            user_id = request["user_id"]
            username = request["username"]

            def update_request_and_assign():
                with self.get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            """
                            UPDATE number_requests
                            SET status = 'approved', processed_at = NOW()
                            WHERE id = %s
                            """,
                            (request_id,),
                        )
                        cursor.execute(
                            """
                            SELECT id, number, name, address, email
                            FROM number_queue
                            WHERE is_used = FALSE AND is_completed = FALSE
                            ORDER BY id LIMIT 1
                            """
                        )
                        rec = cursor.fetchone()
                        if not rec:
                            return None
                        cursor.execute(
                            """
                            UPDATE number_queue
                            SET is_used = TRUE,
                                used_by_user_id = %s,
                                used_by_username = %s,
                                used_at = NOW()
                            WHERE id = %s
                            """,
                            (user_id, username, rec["id"]),
                        )
                        cursor.execute(
                            "DELETE FROM number_requests WHERE id = %s", (request_id,)
                        )
                        return rec

            record = await loop.run_in_executor(None, update_request_and_assign)

            if record:
                user_info = await loop.run_in_executor(
                    None, self.get_user_info, user_id
                )
                agent_name = (
                    user_info["agent_name"]
                    if user_info and user_info.get("agent_name")
                    else "Agent"
                )
                reference = (
                    user_info["reference"] if user_info else self.reference
                )

                dm = ""
                dm += f"👤 <b>Agent:</b> {agent_name}\n"
                dm += f"🗃️ Reference: <code>{reference}</code>\n\n"
                dm += "🎫 <b>Your Line:</b>\n\n"
                if record.get("name"):
                    dm += f"👤 Name: {record['name']}\n"
                dm += f"📞 Number: <code>{record['number']}</code>\n"
                if record.get("address"):
                    dm += f"📍 Address: {record['address']}\n"
                if record.get("email"):
                    dm += f"📧 Email: {record['email']}\n"

                kb = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="OTP 📞", callback_data=f"otp_{user_id}"
                            ),
                            InlineKeyboardButton(
                                text="No Answer ❌",
                                callback_data=f"noanswer_{user_id}",
                            ),
                        ]
                    ]
                )

                try:
                    await self.bot.send_message(
                        chat_id=user_id,
                        text=dm,
                        parse_mode="HTML",
                        reply_markup=kb,
                    )

                    user_mention = (
                        f"@{username}" if username else f"User {user_id}"
                    )
                    await self.bot.send_message(
                        chat_id=self.group_chat_id,
                        text=f"✅ {user_mention} line has been sent in private",
                        parse_mode="HTML",
                    )
                except TelegramForbiddenError:
                    await self.bot.send_message(
                        chat_id=self.admin_id,
                        text=(
                            f"⚠️ Can't send DM to {username or user_id}. "
                            "They need to start the bot first."
                        ),
                        parse_mode="HTML",
                    )
                except Exception as e:
                    logging.error(f"Error sending line: {e}")
            else:
                await self.bot.send_message(
                    chat_id=self.admin_id,
                    text="❌ No numbers available in queue",
                    parse_mode="HTML",
                )

            try:
                await callback.message.delete()
            except Exception:
                pass

            await callback.answer("✅ Line approved and sent!")

        @r.callback_query(F.data.startswith("otp_"))
        async def callback_otp(callback: CallbackQuery):
            user_id = int(callback.data.split("_")[1])

            if callback.from_user.id != user_id:
                await callback.answer("Not for you!", show_alert=True)
                return

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None, self.update_record_status, user_id, "OTP"
            )

            username = callback.from_user.username or callback.from_user.first_name
            user_mention = callback.from_user.mention_html()

            record = await loop.run_in_executor(None, self.get_user_record, user_id)
            user_info = await loop.run_in_executor(None, self.get_user_info, user_id)
            agent_name = (
                user_info["agent_name"]
                if user_info and user_info.get("agent_name")
                else "Agent"
            )
            reference = user_info["reference"] if user_info else self.reference

            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="Need a Pass ⛹️",
                            callback_data=f"need_pass_{user_id}",
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            text="Finishing 🫡",
                            callback_data=f"finishing_{user_id}",
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            text="Vic Needs Callback ☎️",
                            callback_data=f"vic_callback_{user_id}",
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            text="Call Ended 📵",
                            callback_data=f"call_ended_{user_id}",
                        )
                    ],
                ]
            )

            await callback.message.edit_text(
                f"{callback.message.text}\n\n✅ Status: OTP 📞\n\nWhat do you need?",
                reply_markup=kb,
            )

            if record:
                admin_msg = (
                    f"📲 <b>{user_mention} is on call (OTP)</b>\n\n"
                    f"👤 <b>Agent:</b> {agent_name}\n"
                    f"🗃️ Reference: <code>{reference}</code>\n\n"
                    "<b>🎫Line Details:</b>\n\n"
                )
                if record.get("name"):
                    admin_msg += f"👤 Name: {record['name']}\n"
                admin_msg += f"📞 Number: <code>{record['number']}</code>\n"
                if record.get("address"):
                    admin_msg += f"📍 Address: {record['address']}\n"
                if record.get("email"):
                    admin_msg += f"📧 Email: {record['email']}\n"

                try:
                    await self.bot.send_message(
                        chat_id=self.admin_id,
                        text=admin_msg,
                        parse_mode="HTML",
                    )
                except Exception as e:
                    logging.error(f"Error sending to master admin: {e}")

                admins = await loop.run_in_executor(None, self.get_all_admins)
                for adm in admins:
                    if adm["user_id"] == self.admin_id:
                        continue
                    try:
                        await self.bot.send_message(
                            chat_id=adm["user_id"],
                            text=admin_msg,
                            parse_mode="HTML",
                        )
                    except Exception as e:
                        logging.error(
                            f"Error sending to admin {adm['user_id']}: {e}"
                        )

            try:
                await self.bot.send_message(
                    chat_id=self.group_chat_id,
                    text=f"📲 {user_mention} is on call",
                )
            except Exception as e:
                logging.error(f"Error notifying group: {e}")

            await callback.answer()

        @r.callback_query(F.data.startswith("noanswer_"))
        async def callback_noanswer(callback: CallbackQuery):
            user_id = int(callback.data.split("_")[1])

            if callback.from_user.id != user_id:
                await callback.answer("Not for you!", show_alert=True)
                return

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None, self.update_record_status, user_id, "No Answer"
            )

            record = await loop.run_in_executor(None, self.get_user_record, user_id)

            username = callback.from_user.username or callback.from_user.first_name
            user_mention = callback.from_user.mention_html()

            user_info = await loop.run_in_executor(None, self.get_user_info, user_id)
            agent_name = (
                user_info["agent_name"]
                if user_info and user_info.get("agent_name")
                else "Agent"
            )
            reference = user_info["reference"] if user_info else self.reference

            if record:
                await loop.run_in_executor(
                    None,
                    self.save_no_answer_record,
                    user_id,
                    username,
                    agent_name,
                    reference,
                    record.get("number"),
                    record.get("name"),
                    record.get("address"),
                    record.get("email"),
                )

            await loop.run_in_executor(None, self.mark_line_completed, user_id)

            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="Request Another Line 🔄",
                            callback_data="request_line",
                        )
                    ]
                ]
            )

            await callback.message.edit_text(
                f"{callback.message.text}\n\n❌ <b>No Answer</b>\n\nRequest another:",
                parse_mode="HTML",
                reply_markup=kb,
            )
            await callback.answer()

        @r.message(Command("stop"), F.from_user.id == self.admin_id)
        async def cmd_stop(message: Message):
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.set_bot_status, "stopped")
            await message.answer(
                "⏸️ <b>Bot stopped!</b>\n\nUsers can't request lines now.",
                parse_mode="HTML",
            )

        @r.message(Command("reset"), F.from_user.id == self.admin_id)
        async def cmd_reset(message: Message):
            loop = asyncio.get_event_loop()
            count = await loop.run_in_executor(None, self.reset_queue)
            await message.answer(
                f"✅ Reset {count} lines (only non-completed).",
                parse_mode="HTML",
            )

        # ============= ADMIN COMMANDS =============

        @r.message(Command("stats"), F.from_user.id == self.admin_id)
        async def cmd_stats(message: Message):
            loop = asyncio.get_event_loop()
            remaining, used = await loop.run_in_executor(None, self.get_queue_stats)

            await message.answer(
                f"📊 <b>Queue Statistics ({self.reference})</b>\n\n"
                f"📦 Available: <b>{remaining}</b>\n"
                f"✅ Used: <b>{used}</b>\n"
                f"📈 Total: <b>{remaining + used}</b>",
                parse_mode="HTML",
            )

        @r.message(Command("clearrequests"), F.from_user.id == self.admin_id)
        async def cmd_clear_requests(message: Message):
            loop = asyncio.get_event_loop()
            count = await loop.run_in_executor(None, self.clear_pending_requests)
            await message.answer(
                f"✅ Cleared {count} pending requests.", parse_mode="HTML"
            )

        @r.message(Command("clear"), F.from_user.id == self.admin_id)
        async def cmd_clear(message: Message):
            loop = asyncio.get_event_loop()
            count = await loop.run_in_executor(None, self.clear_queue)
            await message.answer(
                f"✅ Deleted {count} numbers from queue.", parse_mode="HTML"
            )

        @r.message(Command("add"), F.from_user.id == self.admin_id)
        async def cmd_add(message: Message, state: FSMContext):
            await state.set_state(AdminStates.waiting_for_numbers)
            await message.answer(
                "📝 Send me numbers (one per line).\n\nSend /done when finished.",
                parse_mode="HTML",
            )
            await state.update_data(numbers=[])

        @r.message(AdminStates.waiting_for_numbers, F.text)
        async def receive_numbers(message: Message, state: FSMContext):
            if message.text == "/done":
                data = await state.get_data()
                numbers = data.get("numbers", [])

                if not numbers:
                    await message.answer("❌ No numbers added!")
                    await state.clear()
                    return

                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self.add_numbers_to_queue, numbers)

                await message.answer(
                    f"✅ Added {len(numbers)} numbers to queue!",
                    parse_mode="HTML",
                )
                await state.clear()
            else:
                lines = message.text.strip().split("\n")
                data = await state.get_data()
                numbers = data.get("numbers", [])
                numbers.extend([line.strip() for line in lines if line.strip()])
                await state.update_data(numbers=numbers)
                await message.answer(
                    f"✅ Added {len(lines)} numbers. Total: {len(numbers)}\n\nSend /done to finish.",
                    parse_mode="HTML",
                )

        @r.message(Command("upload"), F.from_user.id == self.admin_id)
        async def cmd_upload(message: Message, state: FSMContext):
            await state.set_state(AdminStates.waiting_for_file)
            await message.answer(
                "📤 Send me a CSV/TXT file with numbers.\n\n"
                "Format: number,name,address,email\n\n"
                "Send /cancel to abort.",
                parse_mode="HTML",
            )

        @r.message(AdminStates.waiting_for_file, F.document)
        async def receive_file(message: Message, state: FSMContext):
            import csv
            import io

            file = message.document
            if not file.file_name.endswith((".csv", ".txt")):
                await message.answer("❌ Only CSV/TXT files allowed!")
                return

            file_data = await self.bot.download(file)
            content = file_data.read().decode("utf-8")

            reader = csv.reader(io.StringIO(content))
            records = []
            for row in reader:
                if len(row) >= 1:
                    number = row[0].strip()
                    name = row[1].strip() if len(row) > 1 else None
                    address = row[2].strip() if len(row) > 2 else None
                    email = row[3].strip() if len(row) > 3 else None
                    records.append((number, name, address, email))

            if not records:
                await message.answer("❌ No valid records found!")
                await state.clear()
                return

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.add_records_from_csv, records)

            await message.answer(
                f"✅ Uploaded {len(records)} records!", parse_mode="HTML"
            )
            await state.clear()

        @r.message(Command("export_used"), F.from_user.id == self.admin_id)
        async def cmd_export_used(message: Message):
            from aiogram.types import BufferedInputFile

            loop = asyncio.get_event_loop()
            csv_data, count = await loop.run_in_executor(None, self.export_used_numbers)

            if not csv_data:
                await message.answer("ℹ️ No used numbers to export.")
                return

            file = BufferedInputFile(csv_data.encode("utf-8"), filename="used_numbers.csv")
            await message.answer_document(
                document=file,
                caption=f"✅ Exported and deleted {count} completed lines.",
            )

        @r.message(Command("export_unused"), F.from_user.id == self.admin_id)
        async def cmd_export_unused(message: Message):
            from aiogram.types import BufferedInputFile

            loop = asyncio.get_event_loop()
            csv_data, _ = await loop.run_in_executor(None, self.export_unused_numbers)

            if not csv_data:
                await message.answer("ℹ️ No unused numbers to export.")
                return

            file = BufferedInputFile(
                csv_data.encode("utf-8"), filename="unused_numbers.csv"
            )
            await message.answer_document(
                document=file, caption="✅ Exported unused numbers (not deleted)."
            )

        @r.message(Command("export_all"), F.from_user.id == self.admin_id)
        async def cmd_export_all(message: Message):
            from aiogram.types import BufferedInputFile

            loop = asyncio.get_event_loop()
            csv_data, count = await loop.run_in_executor(None, self.export_all_numbers)

            if not csv_data:
                await message.answer("ℹ️ No numbers to export.")
                return

            file = BufferedInputFile(csv_data.encode("utf-8"), filename="all_numbers.csv")
            await message.answer_document(
                document=file, caption=f"✅ Exported and deleted {count} total lines."
            )

        @r.message(Command("export_no_answer"), F.from_user.id == self.admin_id)
        async def cmd_export_no_answer(message: Message):
            from aiogram.types import BufferedInputFile

            loop = asyncio.get_event_loop()
            csv_data, count = await loop.run_in_executor(
                None, self.export_no_answer_records
            )

            if not csv_data:
                await message.answer("ℹ️ No no-answer records to export.")
                return

            file = BufferedInputFile(
                csv_data.encode("utf-8"), filename="no_answer_records.csv"
            )
            await message.answer_document(
                document=file, caption=f"✅ Exported and deleted {count} no-answer records."
            )

        @r.message(Command("listadmins"), F.from_user.id == self.admin_id)
        async def cmd_list_admins(message: Message):
            loop = asyncio.get_event_loop()
            admins = await loop.run_in_executor(None, self.get_all_admins)

            text = f"<b>👑 Admin List ({self.reference})</b>\n\n"
            text += f"<b>Master Admin:</b>\n• ID: <code>{self.admin_id}</code>\n\n"

            if admins:
                text += "<b>Other Admins:</b>\n"
                for adm in admins:
                    if adm["user_id"] != self.admin_id:
                        text += f"• {adm['username']} (ID: <code>{adm['user_id']}</code>)\n"
            else:
                text += "<i>No other admins</i>"

            await message.answer(text, parse_mode=ParseMode.HTML)

        @r.message(Command("addadmin"), F.from_user.id == self.admin_id)
        async def cmd_add_admin(message: Message, state: FSMContext):
            if message.reply_to_message:
                new_admin_id = message.reply_to_message.from_user.id
                new_admin_username = (
                        message.reply_to_message.from_user.username
                        or message.reply_to_message.from_user.first_name
                )

                if new_admin_id == self.admin_id:
                    await message.answer("❌ This user is already the master admin.")
                    return

                loop = asyncio.get_event_loop()

                if await loop.run_in_executor(None, self.is_admin, new_admin_id):
                    await message.answer(f"ℹ️ {new_admin_username} is already an admin.")
                    return

                try:
                    await loop.run_in_executor(
                        None, self.add_admin, new_admin_id, new_admin_username, self.admin_id
                    )

                    await message.answer(
                        f"✅ <b>Admin Added!</b>\n\n"
                        f"User: {new_admin_username}\n"
                        f"ID: <code>{new_admin_id}</code>\n\n"
                        f"They can now approve/decline line requests.",
                        parse_mode=ParseMode.HTML,
                    )

                    try:
                        await self.bot.send_message(
                            chat_id=new_admin_id,
                            text="🎉 You've been promoted to admin! You can now approve line requests.",
                            parse_mode=ParseMode.HTML,
                        )
                    except Exception:
                        pass

                except Exception as e:
                    await message.answer(f"❌ Error adding admin: {e}")

                return

            if message.chat.type == "private":
                await state.set_state(AdminStates.waiting_for_forward)
                await message.answer(
                    "<b>👑 Add Admin</b>\n\n"
                    "Forward me any message from the user you want to make admin.\n\n"
                    "Send /cancel to cancel.",
                    parse_mode=ParseMode.HTML,
                )
            else:
                await message.answer("❌ Reply to a user's message or use this in private chat.")

        @r.message(AdminStates.waiting_for_forward, F.forward_from)
        async def receive_forward_add_admin(message: Message, state: FSMContext):
            new_admin_id = message.forward_from.id
            new_admin_username = (
                    message.forward_from.username or message.forward_from.first_name
            )

            if new_admin_id == self.admin_id:
                await message.answer("❌ This user is already the master admin.")
                await state.clear()
                return

            loop = asyncio.get_event_loop()

            if await loop.run_in_executor(None, self.is_admin, new_admin_id):
                await message.answer(f"ℹ️ {new_admin_username} is already an admin.")
                await state.clear()
                return

            try:
                await loop.run_in_executor(
                    None, self.add_admin, new_admin_id, new_admin_username, self.admin_id
                )
                await message.answer(
                    f"✅ <b>Admin Added!</b>\n\n"
                    f"User: {new_admin_username}\n"
                    f"ID: <code>{new_admin_id}</code>\n\n"
                    f"They can now approve/decline line requests.",
                    parse_mode=ParseMode.HTML,
                )

                try:
                    await self.bot.send_message(
                        chat_id=new_admin_id,
                        text="🎉 You've been promoted to admin! You can now approve line requests.",
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass

            except Exception as e:
                await message.answer(f"❌ Error adding admin: {e}")

            await state.clear()

        @r.message(Command("removeadmin"), F.from_user.id == self.admin_id)
        async def cmd_remove_admin(message: Message, state: FSMContext):
            if message.reply_to_message:
                admin_to_remove = message.reply_to_message.from_user.id
                admin_username = (
                        message.reply_to_message.from_user.username
                        or message.reply_to_message.from_user.first_name
                )

                if admin_to_remove == self.admin_id:
                    await message.answer("❌ Cannot remove master admin.")
                    return

                loop = asyncio.get_event_loop()
                count = await loop.run_in_executor(None, self.remove_admin, admin_to_remove)

                if count > 0:
                    await message.answer(
                        f"✅ <b>Admin Removed!</b>\n\n"
                        f"User: {admin_username}\n"
                        f"ID: <code>{admin_to_remove}</code>\n\n"
                        f"They can no longer approve line requests.",
                        parse_mode=ParseMode.HTML,
                    )

                    try:
                        await self.bot.send_message(
                            chat_id=admin_to_remove,
                            text="ℹ️ Your admin privileges have been removed.",
                            parse_mode=ParseMode.HTML,
                        )
                    except Exception:
                        pass
                else:
                    await message.answer(f"❌ {admin_username} is not an admin.")

                return

            if message.chat.type == "private":
                await state.set_state(AdminStates.waiting_for_remove_forward)
                await message.answer(
                    "<b>👑 Remove Admin</b>\n\n"
                    "Forward me any message from the admin you want to remove.\n\n"
                    "Send /cancel to cancel.",
                    parse_mode=ParseMode.HTML,
                )
            else:
                await message.answer("❌ Reply to an admin's message or use this in private chat.")

        @r.message(AdminStates.waiting_for_remove_forward, F.forward_from)
        async def receive_forward_remove_admin(message: Message, state: FSMContext):
            admin_to_remove = message.forward_from.id
            admin_username = (
                    message.forward_from.username or message.forward_from.first_name
            )

            if admin_to_remove == self.admin_id:
                await message.answer("❌ Cannot remove master admin.")
                await state.clear()
                return

            loop = asyncio.get_event_loop()
            count = await loop.run_in_executor(None, self.remove_admin, admin_to_remove)

            if count > 0:
                await message.answer(
                    f"✅ <b>Admin Removed!</b>\n\n"
                    f"User: {admin_username}\n"
                    f"ID: <code>{admin_to_remove}</code>\n\n"
                    f"They can no longer approve line requests.",
                    parse_mode=ParseMode.HTML,
                )

                try:
                    await self.bot.send_message(
                        chat_id=admin_to_remove,
                        text="ℹ️ Your admin privileges have been removed.",
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass
            else:
                await message.answer(f"❌ {admin_username} is not an admin.")

            await state.clear()

    # ============= RUN BOT =============

    async def start(self):
        logging.info(f"📦 Starting bot for reference {self.reference}...")
        self.init_database()

        self.bot = Bot(
            token=self.bot_token,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML),
        )
        self.dp = Dispatcher(storage=MemoryStorage())
        self.dp.include_router(self.router)

        await self.set_bot_commands()

        logging.info(f"🤖 Bot running for reference {self.reference}")
        await self.dp.start_polling(self.bot)
