import asyncio
import logging
import io
import csv
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
    BotCommandScopeChat,
    BufferedInputFile,
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
    """Reusable bot instance with separate config"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.bot_token: str = config["bot_token"]
        self.admin_id: int = config["admin_id"]
        self.group_chat_id: int = config["group_chat_id"]
        self.reference: str = config.get("reference", "REF001")

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
        logging.info(f"✅ LineBot initialized: {self.reference}")

    # ==================== DATABASE ====================

    @contextmanager
    def get_db_connection(self):
        conn = pymysql.connect(**self.db_config)
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.error(f"[{self.reference}] DB error: {e}")
            raise
        finally:
            conn.close()

    def init_database(self):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS bot_status (
                        id INT PRIMARY KEY DEFAULT 1,
                        status VARCHAR(50) DEFAULT 'running'
                    )
                """)
                cursor.execute("""
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
                        INDEX idx_user_id (user_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS users (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        user_id BIGINT NOT NULL UNIQUE,
                        agent_name VARCHAR(255),
                        reference VARCHAR(50) DEFAULT '{self.reference}',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_user_id (user_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                cursor.execute("""
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
                        INDEX idx_is_used (is_used),
                        INDEX idx_is_completed (is_completed)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS number_requests (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        username VARCHAR(255) DEFAULT NULL,
                        group_chat_id BIGINT DEFAULT NULL,
                        status VARCHAR(50) DEFAULT 'pending',
                        requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        processed_at TIMESTAMP NULL DEFAULT NULL,
                        INDEX idx_status (status)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS admins (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        user_id BIGINT NOT NULL UNIQUE,
                        username VARCHAR(255),
                        added_by BIGINT NOT NULL,
                        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
        logging.info(f"[{self.reference}] Database initialized")

    # ==================== DB HELPERS ====================

    def get_bot_status(self) -> str:
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT status FROM bot_status WHERE id = 1")
                row = cursor.fetchone()
                return row["status"] if row else "running"

    def set_bot_status(self, status: str):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO bot_status (id, status) VALUES (1, %s) ON DUPLICATE KEY UPDATE status = %s",
                    (status, status)
                )

    def save_agent_name(self, user_id: int, agent_name: str):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""INSERT INTO users (user_id, agent_name, reference) 
                        VALUES (%s, %s, '{self.reference}') 
                        ON DUPLICATE KEY UPDATE agent_name = %s""",
                    (user_id, agent_name, agent_name)
                )

    def get_user_info(self, user_id: int) -> Optional[Dict]:
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT agent_name, reference FROM users WHERE user_id = %s", (user_id,))
                return cursor.fetchone()

    def get_user_record(self, user_id: int) -> Optional[Dict]:
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """SELECT * FROM number_queue
                       WHERE used_by_user_id = %s AND is_used = TRUE AND is_completed = FALSE
                       ORDER BY used_at DESC, id DESC LIMIT 1""",
                    (user_id,)
                )
                return cursor.fetchone()

    def mark_line_completed(self, user_id: int):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE number_queue SET is_completed = TRUE WHERE used_by_user_id = %s AND is_used = TRUE",
                    (user_id,)
                )
                cursor.execute(
                    "DELETE FROM number_requests WHERE user_id = %s AND status IN ('pending', 'approved')",
                    (user_id,)
                )

    def update_record_status(self, user_id: int, status: str):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE number_queue SET status = %s WHERE used_by_user_id = %s AND is_used = TRUE AND is_completed = FALSE",
                    (status, user_id)
                )

    def update_record_summary(self, user_id: int, summary: str):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """UPDATE number_queue SET call_summary = %s, summary_submitted_at = NOW() 
                       WHERE used_by_user_id = %s AND is_used = TRUE AND is_completed = FALSE""",
                    (summary, user_id)
                )

    def save_no_answer_record(self, user_id, username, agent_name, reference, number, name, address, email):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO no_answer_records 
                       (user_id, username, agent_name, reference, number, name, address, email) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (user_id, username, agent_name, reference, number, name, address, email)
                )

    def is_admin(self, user_id: int) -> bool:
        if user_id == self.admin_id:
            return True
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id FROM admins WHERE user_id = %s", (user_id,))
                return cursor.fetchone() is not None

    def get_all_admins(self) -> List[Dict]:
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT user_id, username, added_at FROM admins ORDER BY added_at")
                return cursor.fetchall()

    def add_admin(self, user_id: int, username: str, added_by: int):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO admins (user_id, username, added_by) VALUES (%s, %s, %s)",
                    (user_id, username, added_by)
                )

    def remove_admin(self, user_id: int) -> int:
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM admins WHERE user_id = %s", (user_id,))
                return cursor.rowcount

    def get_queue_stats(self):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) as c FROM number_queue WHERE is_used = FALSE AND is_completed = FALSE")
                remaining = cursor.fetchone()["c"]
                cursor.execute("SELECT COUNT(*) as c FROM number_queue WHERE is_used = TRUE")
                used = cursor.fetchone()["c"]
                return remaining, used

    def add_numbers_to_queue(self, numbers: List[str]):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.executemany("INSERT INTO number_queue (number) VALUES (%s)", [(n.strip(),) for n in numbers])

    def add_records_from_csv(self, records: List[tuple]):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(
                    "INSERT INTO number_queue (number, name, address, email) VALUES (%s, %s, %s, %s)",
                    records
                )

    def reset_queue(self) -> int:
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """UPDATE number_queue SET is_used = FALSE, used_by_user_id = NULL, 
                       used_by_username = NULL, used_at = NULL, status = NULL, call_summary = NULL
                       WHERE is_completed = FALSE"""
                )
                return cursor.rowcount

    def clear_queue(self) -> int:
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM number_queue")
                cursor.execute("DELETE FROM number_requests")
                return cursor.rowcount

    def clear_pending_requests(self) -> int:
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM number_requests WHERE status IN ('pending', 'approved')")
                return cursor.rowcount

    def export_used_numbers(self):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """SELECT number, name, address, email, used_by_username, used_by_user_id, 
                       status, call_summary, used_at, summary_submitted_at 
                       FROM number_queue WHERE is_completed = TRUE ORDER BY used_at"""
                )
                results = cursor.fetchall()
                if not results:
                    return None, 0

                output = io.StringIO()
                output.write("Number,Name,Address,Email,Used By,User ID,Status,Summary,Used At,Summary At\n")
                for row in results:
                    status = row["status"] or "-"
                    summary = (row["call_summary"] or "-").replace('"', '""')
                    summary_at = row["summary_submitted_at"] or "-"
                    name = row["name"] or "-"
                    address = (row["address"] or "-").replace('"', '""')
                    email = row["email"] or "-"
                    output.write(f'{row["number"]},{name},"{address}",{email},{row["used_by_username"]},{row["used_by_user_id"]},{status},"{summary}",{row["used_at"]},{summary_at}\n')

                cursor.execute("DELETE FROM number_queue WHERE is_completed = TRUE")
                return output.getvalue(), cursor.rowcount

    def export_unused_numbers(self):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT number, name, address, email FROM number_queue WHERE is_used = FALSE AND is_completed = FALSE ORDER BY id"
                )
                results = cursor.fetchall()
                if not results:
                    return None, 0

                output = io.StringIO()
                output.write("Number,Name,Address,Email\n")
                for row in results:
                    name = row["name"] or "-"
                    address = (row["address"] or "-").replace('"', '""')
                    email = row["email"] or "-"
                    output.write(f'{row["number"]},{name},"{address}",{email}\n')
                return output.getvalue(), len(results)

    def export_no_answer_records(self):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM no_answer_records ORDER BY created_at DESC"
                )
                results = cursor.fetchall()
                if not results:
                    return None, 0

                output = io.StringIO()
                output.write("User ID,Username,Agent Name,Reference,Number,Name,Address,Email,Date\n")
                for row in results:
                    name = row["name"] or "-"
                    address = (row["address"] or "-").replace('"', '""')
                    email = row["email"] or "-"
                    output.write(f'{row["user_id"]},{row["username"]},{row["agent_name"]},{row["reference"]},{row["number"]},{name},"{address}",{email},{row["created_at"]}\n')

                cursor.execute("DELETE FROM no_answer_records")
                return output.getvalue(), cursor.rowcount

    # ==================== COMMANDS SETUP ====================

    async def set_bot_commands(self):
        admin_commands = [
            BotCommand(command="start", description="Start the bot"),
            BotCommand(command="stats", description="View queue statistics"),
            BotCommand(command="add", description="Add numbers manually"),
            BotCommand(command="upload", description="Upload CSV/TXT file"),
            BotCommand(command="reset", description="Reset all numbers"),
            BotCommand(command="clear", description="Delete all numbers"),
            BotCommand(command="clearrequests", description="Clear pending requests"),
            BotCommand(command="export_used", description="Export used numbers"),
            BotCommand(command="export_unused", description="Export unused numbers"),
            BotCommand(command="export_no_answer", description="Export no answer records"),
            BotCommand(command="addadmin", description="Add new admin"),
            BotCommand(command="removeadmin", description="Remove admin"),
            BotCommand(command="listadmins", description="List all admins"),
            BotCommand(command="stop", description="Stop the bot"),
        ]
        user_commands = [BotCommand(command="start", description="Start the bot")]

        await self.bot.set_my_commands(admin_commands, scope=BotCommandScopeChat(chat_id=self.admin_id))
        for adm in self.get_all_admins():
            if adm["user_id"] != self.admin_id:
                try:
                    await self.bot.set_my_commands(admin_commands, scope=BotCommandScopeChat(chat_id=adm["user_id"]))
                except:
                    pass
        await self.bot.set_my_commands(user_commands)

    # ==================== HANDLERS ====================

    def _register_handlers(self):
        r = self.router

        # /start
        @r.message(Command("start"))
        async def cmd_start(message: Message, state: FSMContext):
            if message.chat.type != "private":
                await message.answer("👋 Use /start in private chat!")
                return

            if message.from_user.id == self.admin_id:
                status = self.get_bot_status()
                if status == "stopped":
                    self.set_bot_status("running")
                    await message.answer("▶️ <b>Bot restarted!</b>", parse_mode="HTML")
                else:
                    await message.answer(
                        f"🤖 <b>Admin Panel ({self.reference})</b>\n\n"
                        "/stats - Queue stats\n/add - Add numbers\n/upload - Upload file\n"
                        "/export_used - Export used\n/export_unused - Export remaining",
                        parse_mode="HTML"
                    )
                return

            try:
                member = await self.bot.get_chat_member(self.group_chat_id, message.from_user.id)
                if member.status in ["kicked", "left"]:
                    await message.answer("⚠️ You must be a member of the group!")
                    return
            except:
                await message.answer("⚠️ You must be a member of the group!")
                return

            loop = asyncio.get_event_loop()
            user_info = await loop.run_in_executor(None, self.get_user_info, message.from_user.id)

            if user_info and user_info.get("agent_name"):
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="Line 📞", callback_data="request_line")]
                ])
                await message.answer(
                    f"👋 Welcome back, <b>{user_info['agent_name']}</b>!\n\n"
                    f"🗃️ Reference: <code>{user_info['reference']}</code>\n\n"
                    "Press the button below to request a line",
                    parse_mode="HTML", reply_markup=kb
                )
            else:
                await state.set_state(UserStates.waiting_for_agent_name)
                await message.answer("👋 Welcome! Please enter your agent name:")

        # Agent name
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
            await loop.run_in_executor(None, self.save_agent_name, message.from_user.id, agent_name)

            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Line 📞", callback_data="request_line")]
            ])
            await message.answer(
                f"✅ <b>Agent name saved!</b>\n\n👤 <b>Agent:</b> {agent_name}\n"
                f"🗃️ Reference: <code>{self.reference}</code>",
                parse_mode="HTML", reply_markup=kb
            )
            await state.clear()

        # Request line
        @r.callback_query(F.data == "request_line")
        async def callback_request_line(callback: CallbackQuery):
            user_id = callback.from_user.id
            username = callback.from_user.username or callback.from_user.first_name

            status = self.get_bot_status()
            if status == "stopped":
                await callback.message.answer("⏸️ Bot is currently stopped.")
                await callback.answer()
                return

            loop = asyncio.get_event_loop()

            # Check existing line
            existing = await loop.run_in_executor(None, self.get_user_record, user_id)
            if existing:
                await callback.message.answer("❌ You already have an active line!")
                await callback.answer()
                return

            # Assign new line
            def assign_line():
                with self.get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            "SELECT id, number, name, address, email FROM number_queue WHERE is_used = FALSE AND is_completed = FALSE ORDER BY id LIMIT 1"
                        )
                        rec = cursor.fetchone()
                        if rec:
                            cursor.execute(
                                "UPDATE number_queue SET is_used = TRUE, used_by_user_id = %s, used_by_username = %s, used_at = NOW() WHERE id = %s",
                                (user_id, username, rec["id"])
                            )
                        return rec

            record = await loop.run_in_executor(None, assign_line)

            if not record:
                await callback.message.answer("❌ No lines available!")
                await callback.answer()
                return

            user_info = await loop.run_in_executor(None, self.get_user_info, user_id)
            agent_name = user_info["agent_name"] if user_info else "Agent"
            reference = user_info["reference"] if user_info else self.reference

            dm = f"👤 <b>Agent:</b> {agent_name}\n🗃️ Reference: <code>{reference}</code>\n\n🎫 <b>Your Line:</b>\n\n"
            if record.get("name"):
                dm += f"👤 Name: {record['name']}\n"
            dm += f"📞 Number: <code>{record['number']}</code>\n"
            if record.get("address"):
                dm += f"📍 Address: {record['address']}\n"
            if record.get("email"):
                dm += f"📧 Email: {record['email']}\n"

            kb = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="OTP 📞", callback_data=f"otp_{user_id}"),
                    InlineKeyboardButton(text="No Answer ❌", callback_data=f"noanswer_{user_id}")
                ]
            ])

            await callback.message.answer(dm, parse_mode="HTML", reply_markup=kb)

            try:
                user_mention = callback.from_user.mention_html()
                await self.bot.send_message(self.group_chat_id, f"✅ {user_mention} got a line", parse_mode="HTML")
            except:
                pass

            await callback.answer()

        # OTP
        @r.callback_query(F.data.startswith("otp_"))
        async def callback_otp(callback: CallbackQuery):
            user_id = int(callback.data.split("_")[1])
            if callback.from_user.id != user_id:
                await callback.answer("Not for you!", show_alert=True)
                return

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.update_record_status, user_id, "OTP")

            record = await loop.run_in_executor(None, self.get_user_record, user_id)
            user_info = await loop.run_in_executor(None, self.get_user_info, user_id)
            agent_name = user_info["agent_name"] if user_info else "Agent"
            reference = user_info["reference"] if user_info else self.reference

            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Need a Pass ⛹️", callback_data=f"need_pass_{user_id}")],
                [InlineKeyboardButton(text="Needs an email 📧", callback_data=f"need_email_{user_id}")],
                [InlineKeyboardButton(text="Finishing 🫡", callback_data=f"finishing_{user_id}")],
                [InlineKeyboardButton(text="Vic Needs Callback ☎️", callback_data=f"vic_callback_{user_id}")],
                [InlineKeyboardButton(text="Call Ended 📵", callback_data=f"call_ended_{user_id}")]
            ])

            await callback.message.edit_text(
                f"{callback.message.text}\n\n✅ Status: OTP 📞\n\nWhat do you need?",
                reply_markup=kb
            )

            if record:
                user_mention = callback.from_user.mention_html()
                admin_msg = f"📲 <b>{user_mention} is on call (OTP)</b>\n\n"
                admin_msg += f"👤 <b>Agent:</b> {agent_name}\n🗃️ Reference: <code>{reference}</code>\n\n"
                admin_msg += "<b>🎫 Line Details:</b>\n"
                if record.get("name"):
                    admin_msg += f"👤 Name: {record['name']}\n"
                admin_msg += f"📞 Number: <code>{record['number']}</code>\n"
                if record.get("address"):
                    admin_msg += f"📍 Address: {record['address']}\n"
                if record.get("email"):
                    admin_msg += f"📧 Email: {record['email']}\n"

                try:
                    await self.bot.send_message(self.admin_id, admin_msg, parse_mode="HTML")
                except:
                    pass

                for adm in await loop.run_in_executor(None, self.get_all_admins):
                    if adm["user_id"] != self.admin_id:
                        try:
                            await self.bot.send_message(adm["user_id"], admin_msg, parse_mode="HTML")
                        except:
                            pass

            try:
                await self.bot.send_message(self.group_chat_id, f"📲 {callback.from_user.mention_html()} is on call", parse_mode="HTML")
            except:
                pass

            await callback.answer()

        # No Answer
        @r.callback_query(F.data.startswith("noanswer_"))
        async def callback_noanswer(callback: CallbackQuery):
            user_id = int(callback.data.split("_")[1])
            if callback.from_user.id != user_id:
                await callback.answer("Not for you!", show_alert=True)
                return

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.update_record_status, user_id, "No Answer")

            # Get record BEFORE marking completed
            record = await loop.run_in_executor(None, self.get_user_record, user_id)

            username = callback.from_user.username or callback.from_user.first_name
            user_info = await loop.run_in_executor(None, self.get_user_info, user_id)
            agent_name = user_info["agent_name"] if user_info else "Agent"
            reference = user_info["reference"] if user_info else self.reference

            if record:
                await loop.run_in_executor(
                    None, self.save_no_answer_record,
                    user_id, username, agent_name, reference,
                    record.get("number"), record.get("name"), record.get("address"), record.get("email")
                )

            # NOW mark completed
            await loop.run_in_executor(None, self.mark_line_completed, user_id)

            try:
                await self.bot.send_message(self.group_chat_id, f"❌ {callback.from_user.mention_html()} - No Answer", parse_mode="HTML")
            except:
                pass

            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Request Another Line 🔄", callback_data="request_line")]
            ])
            await callback.message.edit_text(
                f"{callback.message.text}\n\n❌ <b>No Answer</b>\n\nRequest another:",
                parse_mode="HTML", reply_markup=kb
            )
            await callback.answer()

        # Call Ended
        @r.callback_query(F.data.startswith("call_ended_"))
        async def callback_call_ended(callback: CallbackQuery, state: FSMContext):
            user_id = int(callback.data.split("_")[2])
            if callback.from_user.id != user_id:
                await callback.answer("Not for you!", show_alert=True)
                return

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.update_record_status, user_id, "Call Ended")

            await state.set_state(UserStates.waiting_for_call_ended_summary)
            await callback.message.edit_text(
                f"{callback.message.text}\n\n📵 <b>Call Ended</b>\n\nPlease send a summary:",
                parse_mode="HTML"
            )
            await callback.answer()

        @r.message(UserStates.waiting_for_call_ended_summary, F.text)
        async def receive_call_ended_summary(message: Message, state: FSMContext):
            user_id = message.from_user.id
            summary = message.text.strip()

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.update_record_summary, user_id, summary)
            await loop.run_in_executor(None, self.mark_line_completed, user_id)

            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Request Another Line 🔄", callback_data="request_line")]
            ])
            await message.answer("✅ <b>Summary saved!</b>\n\nRequest another line:", parse_mode="HTML", reply_markup=kb)

            try:
                await self.bot.send_message(self.group_chat_id, f"📵 {message.from_user.mention_html()} - Call Ended", parse_mode="HTML")
            except:
                pass

            await state.clear()

        # Finishing
        @r.callback_query(F.data.startswith("finishing_"))
        async def callback_finishing(callback: CallbackQuery, state: FSMContext):
            user_id = int(callback.data.split("_")[1])
            if callback.from_user.id != user_id:
                await callback.answer("Not for you!", show_alert=True)
                return

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.update_record_status, user_id, "Finishing")

            await state.set_state(UserStates.waiting_for_finishing_summary)
            await callback.message.edit_text(
                f"{callback.message.text}\n\n🫡 <b>Finishing</b>\n\nPlease send a summary:",
                parse_mode="HTML"
            )
            await callback.answer()

        @r.message(UserStates.waiting_for_finishing_summary, F.text)
        async def receive_finishing_summary(message: Message, state: FSMContext):
            user_id = message.from_user.id
            summary = message.text.strip()

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.update_record_summary, user_id, summary)
            await loop.run_in_executor(None, self.mark_line_completed, user_id)

            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Request Another Line 🔄", callback_data="request_line")]
            ])
            await message.answer("✅ <b>Summary saved!</b>\n\nRequest another line:", parse_mode="HTML", reply_markup=kb)

            try:
                await self.bot.send_message(self.group_chat_id, f"🫡 {message.from_user.mention_html()} - Finishing", parse_mode="HTML")
            except:
                pass

            await state.clear()

        # Vic Callback
        @r.callback_query(F.data.startswith("vic_callback_"))
        async def callback_vic_callback(callback: CallbackQuery, state: FSMContext):
            user_id = int(callback.data.split("_")[2])
            if callback.from_user.id != user_id:
                await callback.answer("Not for you!", show_alert=True)
                return

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.update_record_status, user_id, "Callback")

            await state.set_state(UserStates.waiting_for_callback_summary)
            await callback.message.edit_text(
                f"{callback.message.text}\n\n☎️ <b>Vic Needs Callback</b>\n\nPlease send details:",
                parse_mode="HTML"
            )
            await callback.answer()

        @r.message(UserStates.waiting_for_callback_summary, F.text)
        async def receive_callback_summary(message: Message, state: FSMContext):
            user_id = message.from_user.id
            summary = message.text.strip()

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.update_record_summary, user_id, summary)
            await loop.run_in_executor(None, self.mark_line_completed, user_id)

            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Request Another Line 🔄", callback_data="request_line")]
            ])
            await message.answer("✅ <b>Callback saved!</b>\n\nRequest another line:", parse_mode="HTML", reply_markup=kb)

            try:
                await self.bot.send_message(self.group_chat_id, f"☎️ {message.from_user.mention_html()} - Callback Requested", parse_mode="HTML")
            except:
                pass

            await state.clear()

        # Need Pass
        # Need Pass (updated to match email flow)
        @r.callback_query(F.data.startswith("need_pass_"))
        async def callback_need_pass(callback: CallbackQuery):
            user_id = int(callback.data.split("_")[2])
            if callback.from_user.id != user_id:
                await callback.answer("Not for you!", show_alert=True)
                return

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.update_record_status, user_id, "Need Pass")

            try:
                await self.bot.send_message(
                    self.group_chat_id,
                    f"⛹️ <b>User needs a pass</b>\n\n{callback.from_user.mention_html()}",
                    parse_mode="HTML"
                )
            except Exception as e:
                logging.error(f"[{self.reference}] Group notify error: {e}")

            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Needs an email 📧", callback_data=f"need_email_{user_id}")],
                [InlineKeyboardButton(text="Finishing 🫡", callback_data=f"finishing_{user_id}")],
                [InlineKeyboardButton(text="Vic Needs Callback ☎️", callback_data=f"vic_callback_{user_id}")],
                [InlineKeyboardButton(text="Call Ended 📵", callback_data=f"call_ended_{user_id}")]
            ])

            await callback.message.edit_text(
                f"{callback.message.text}\n\n⛹️ <b>Pass Requested</b>",
                parse_mode="HTML",
                reply_markup=kb
            )
            await callback.answer()

        # Needs Email
        @r.callback_query(F.data.startswith("need_email_"))
        async def callback_need_email(callback: CallbackQuery):
            user_id = int(callback.data.split("_")[2])
            if callback.from_user.id != user_id:
                await callback.answer("Not for you!", show_alert=True)
                return

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.update_record_status, user_id, "Need Email")

            try:
                await self.bot.send_message(
                    self.group_chat_id,
                    f"📧 <b>User needs email sent</b>\n\n{callback.from_user.mention_html()}",
                    parse_mode="HTML"
                )
            except Exception as e:
                logging.error(f"[{self.reference}] Group notify error: {e}")

            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Finishing 🫡", callback_data=f"finishing_{user_id}")],
                [InlineKeyboardButton(text="Vic Needs Callback ☎️", callback_data=f"vic_callback_{user_id}")],
                [InlineKeyboardButton(text="Call Ended 📵", callback_data=f"call_ended_{user_id}")]
            ])

            await callback.message.edit_text(
                f"{callback.message.text}\n\n📧 <b>Email Requested</b>",
                parse_mode="HTML",
                reply_markup=kb
            )
            await callback.answer()

        # ==================== ADMIN COMMANDS ====================

        @r.message(Command("stats"))
        async def cmd_stats(message: Message):
            if not self.is_admin(message.from_user.id):
                return
            loop = asyncio.get_event_loop()
            remaining, used = await loop.run_in_executor(None, self.get_queue_stats)
            await message.answer(
                f"📊 <b>Queue Stats ({self.reference})</b>\n\n"
                f"📦 Available: <b>{remaining}</b>\n✅ Used: <b>{used}</b>\n📈 Total: <b>{remaining + used}</b>",
                parse_mode="HTML"
            )

        @r.message(Command("add"))
        async def cmd_add(message: Message, state: FSMContext):
            if not self.is_admin(message.from_user.id):
                return
            await state.set_state(AdminStates.waiting_for_numbers)
            await state.update_data(numbers=[])
            await message.answer("📝 Send numbers (one per line). Send /done when finished.")

        @r.message(Command("done"), AdminStates.waiting_for_numbers)
        async def cmd_done(message: Message, state: FSMContext):
            data = await state.get_data()
            numbers = data.get("numbers", [])
            if not numbers:
                await message.answer("❌ No numbers added!")
                await state.clear()
                return

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.add_numbers_to_queue, numbers)
            await message.answer(f"✅ Added {len(numbers)} numbers!")
            await state.clear()

        @r.message(AdminStates.waiting_for_numbers, F.text)
        async def receive_numbers(message: Message, state: FSMContext):
            if message.text.startswith("/"):
                return
            lines = [l.strip() for l in message.text.strip().split("\n") if l.strip()]
            data = await state.get_data()
            numbers = data.get("numbers", [])
            numbers.extend(lines)
            await state.update_data(numbers=numbers)
            await message.answer(f"✅ Added {len(lines)}. Total: {len(numbers)}. Send /done to finish.")

        @r.message(Command("upload"))
        async def cmd_upload(message: Message, state: FSMContext):
            if not self.is_admin(message.from_user.id):
                return
            await state.set_state(AdminStates.waiting_for_file)
            await message.answer("📤 Send a CSV/TXT file. Format: number,name,address,email")

        @r.message(AdminStates.waiting_for_file, F.document)
        async def receive_file(message: Message, state: FSMContext):
            file = message.document
            if not file.file_name.endswith((".csv", ".txt")):
                await message.answer("❌ Only CSV/TXT files!")
                return

            file_data = await self.bot.download(file)
            content = file_data.read().decode("utf-8")
            reader = csv.reader(io.StringIO(content))
            records = []
            for row in reader:
                if row:
                    number = row[0].strip()
                    name = row[1].strip() if len(row) > 1 else None
                    address = row[2].strip() if len(row) > 2 else None
                    email = row[3].strip() if len(row) > 3 else None
                    records.append((number, name, address, email))

            if not records:
                await message.answer("❌ No records found!")
                await state.clear()
                return

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.add_records_from_csv, records)
            await message.answer(f"✅ Uploaded {len(records)} records!")
            await state.clear()

        @r.message(Command("reset"))
        async def cmd_reset(message: Message):
            if message.from_user.id != self.admin_id:
                return
            loop = asyncio.get_event_loop()
            count = await loop.run_in_executor(None, self.reset_queue)
            await message.answer(f"✅ Reset {count} lines.")

        @r.message(Command("clear"))
        async def cmd_clear(message: Message):
            if message.from_user.id != self.admin_id:
                return
            loop = asyncio.get_event_loop()
            count = await loop.run_in_executor(None, self.clear_queue)
            await message.answer(f"✅ Deleted {count} numbers.")

        @r.message(Command("clearrequests"))
        async def cmd_clear_requests(message: Message):
            if message.from_user.id != self.admin_id:
                return
            loop = asyncio.get_event_loop()
            count = await loop.run_in_executor(None, self.clear_pending_requests)
            await message.answer(f"✅ Cleared {count} requests.")

        @r.message(Command("stop"))
        async def cmd_stop(message: Message):
            if message.from_user.id != self.admin_id:
                return
            self.set_bot_status("stopped")
            await message.answer("⏸️ Bot stopped!")

        @r.message(Command("export_used"))
        async def cmd_export_used(message: Message):
            if not self.is_admin(message.from_user.id):
                return
            loop = asyncio.get_event_loop()
            csv_data, count = await loop.run_in_executor(None, self.export_used_numbers)
            if not csv_data:
                await message.answer("ℹ️ No used numbers.")
                return
            file = BufferedInputFile(csv_data.encode(), filename="used_numbers.csv")
            await message.answer_document(file, caption=f"✅ Exported {count} lines.")

        @r.message(Command("export_unused"))
        async def cmd_export_unused(message: Message):
            if not self.is_admin(message.from_user.id):
                return
            loop = asyncio.get_event_loop()
            csv_data, count = await loop.run_in_executor(None, self.export_unused_numbers)
            if not csv_data:
                await message.answer("ℹ️ No unused numbers.")
                return
            file = BufferedInputFile(csv_data.encode(), filename="unused_numbers.csv")
            await message.answer_document(file, caption=f"✅ Exported {count} lines.")

        @r.message(Command("export_no_answer"))
        async def cmd_export_no_answer(message: Message):
            if not self.is_admin(message.from_user.id):
                return
            loop = asyncio.get_event_loop()
            csv_data, count = await loop.run_in_executor(None, self.export_no_answer_records)
            if not csv_data:
                await message.answer("ℹ️ No records.")
                return
            file = BufferedInputFile(csv_data.encode(), filename="no_answer.csv")
            await message.answer_document(file, caption=f"✅ Exported {count} records.")

        @r.message(Command("listadmins"))
        async def cmd_listadmins(message: Message):
            if message.from_user.id != self.admin_id:
                return
            loop = asyncio.get_event_loop()
            admins = await loop.run_in_executor(None, self.get_all_admins)
            text = f"<b>👑 Admins ({self.reference})</b>\n\nMaster: <code>{self.admin_id}</code>\n\n"
            if admins:
                for a in admins:
                    text += f"• {a['username']} (<code>{a['user_id']}</code>)\n"
            await message.answer(text, parse_mode="HTML")

        @r.message(Command("addadmin"))
        async def cmd_addadmin(message: Message, state: FSMContext):
            if message.from_user.id != self.admin_id:
                return
            if message.reply_to_message:
                new_id = message.reply_to_message.from_user.id
                new_name = message.reply_to_message.from_user.username or message.reply_to_message.from_user.first_name
                if self.is_admin(new_id):
                    await message.answer("Already an admin!")
                    return
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self.add_admin, new_id, new_name, self.admin_id)
                await message.answer(f"✅ Added {new_name} as admin!")
                return
            await state.set_state(AdminStates.waiting_for_forward)
            await message.answer("Forward a message from the user to add as admin.")

        @r.message(AdminStates.waiting_for_forward, F.forward_from)
        async def receive_forward_admin(message: Message, state: FSMContext):
            new_id = message.forward_from.id
            new_name = message.forward_from.username or message.forward_from.first_name
            if self.is_admin(new_id):
                await message.answer("Already an admin!")
                await state.clear()
                return
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.add_admin, new_id, new_name, self.admin_id)
            await message.answer(f"✅ Added {new_name} as admin!")
            await state.clear()

        @r.message(Command("removeadmin"))
        async def cmd_removeadmin(message: Message, state: FSMContext):
            if message.from_user.id != self.admin_id:
                return
            if message.reply_to_message:
                rm_id = message.reply_to_message.from_user.id
                loop = asyncio.get_event_loop()
                count = await loop.run_in_executor(None, self.remove_admin, rm_id)
                await message.answer("✅ Removed!" if count else "❌ Not an admin.")
                return
            await state.set_state(AdminStates.waiting_for_remove_forward)
            await message.answer("Forward a message from the admin to remove.")

        @r.message(AdminStates.waiting_for_remove_forward, F.forward_from)
        async def receive_forward_remove(message: Message, state: FSMContext):
            rm_id = message.forward_from.id
            loop = asyncio.get_event_loop()
            count = await loop.run_in_executor(None, self.remove_admin, rm_id)
            await message.answer("✅ Removed!" if count else "❌ Not an admin.")
            await state.clear()

    # ==================== RUN ====================

    async def start(self):
        logging.info(f"[{self.reference}] Starting...")
        self.init_database()

        self.bot = Bot(token=self.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        self.dp = Dispatcher(storage=MemoryStorage())
        self.dp.include_router(self.router)

        await self.set_bot_commands()
        logging.info(f"[{self.reference}] Bot running!")
        await self.dp.start_polling(self.bot)
