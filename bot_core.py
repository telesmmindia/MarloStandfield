import asyncio
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, BotCommand
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramForbiddenError
import pymysql
from pymysql.cursors import DictCursor
import logging
from contextlib import contextmanager
import io

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


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
    """Reusable bot instance with separate configs"""

    def __init__(self, config: dict):
        self.config = config
        self.bot_token = config['bot_token']
        self.admin_id = config['admin_id']
        self.group_chat_id = config['group_chat_id']
        self.reference = config['reference']
        self.db_config = {**config['database'], 'cursorclass': DictCursor}

        self.bot = None
        self.dp = None
        self.router = Router()

        # Register all handlers
        self._register_handlers()

        logging.info(f"✅ Bot initialized with reference {self.reference}")

    @contextmanager
    def get_db_connection(self):
        connection = pymysql.connect(**self.db_config)
        try:
            yield connection
            connection.commit()
        except Exception as e:
            connection.rollback()
            logging.error(f"Database error: {e}")
            raise
        finally:
            connection.close()

    def init_database(self):
        """Initialize database tables"""
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
                        INDEX idx_user_id (user_id),
                        INDEX idx_created_at (created_at)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """)
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS users (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        user_id BIGINT NOT NULL UNIQUE,
                        agent_name VARCHAR(255),
                        reference VARCHAR(50) DEFAULT '{self.reference}',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_user_id (user_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
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
                        group_chat_id BIGINT DEFAULT NULL,
                        INDEX idx_is_used (is_used),
                        INDEX idx_is_completed (is_completed),
                        INDEX idx_added_at (added_at),
                        INDEX idx_user_id (used_by_user_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """)
                cursor.execute("""
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
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS admins (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        user_id BIGINT NOT NULL UNIQUE,
                        username VARCHAR(255),
                        added_by BIGINT NOT NULL,
                        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_user_id (user_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """)
                logging.info(f"✅ Database tables initialized for {self.reference}")

    # Database helper functions
    def save_agent_name(self, user_id, agent_name):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""INSERT INTO users (user_id, agent_name, reference) 
                       VALUES (%s, %s, '{self.reference}') 
                       ON DUPLICATE KEY UPDATE agent_name = %s""",
                    (user_id, agent_name, agent_name)
                )

    def get_user_info(self, user_id):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT agent_name, reference FROM users WHERE user_id = %s", (user_id,))
                return cursor.fetchone()

    def get_user_record(self, user_id: int):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """SELECT * FROM number_queue
                       WHERE used_by_user_id = %s
                         AND is_used = TRUE
                         AND is_completed = FALSE
                       ORDER BY used_at DESC, id DESC
                       LIMIT 1""",
                    (user_id,)
                )
                return cursor.fetchone()

    def mark_line_completed(self, user_id):
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

    def is_admin(self, user_id):
        if user_id == self.admin_id:
            return True
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id FROM admins WHERE user_id = %s", (user_id,))
                return cursor.fetchone() is not None

    def get_all_admins(self):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT user_id, username, added_at FROM admins ORDER BY added_at")
                return cursor.fetchall()

    def update_record_status(self, user_id: int, status: str):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE number_queue SET status = %s WHERE used_by_user_id = %s",
                    (status, user_id)
                )

    # Add all your other database functions here...
    # (get_queue_stats, export_used_numbers, etc.)

    def _register_handlers(self):
        """Register all bot handlers"""

        @self.router.message(Command("start"))
        async def cmd_start(message: Message, state: FSMContext):
            if message.chat.type == "private":
                if message.from_user.id == self.admin_id:
                    await message.answer(
                        f"🤖 <b>Admin Panel - {self.reference}</b>\n\n"
                        "Use /stats, /add, /upload, /export_used, etc.",
                        parse_mode="HTML"
                    )
                else:
                    # Check group membership
                    try:
                        member = await self.bot.get_chat_member(self.group_chat_id, message.from_user.id)
                        if member.status in ["kicked", "left"]:
                            await message.answer("⚠️ Join the group first!")
                            return
                    except:
                        await message.answer("⚠️ Join the group first!")
                        return

                    # Check if user has agent name
                    loop = asyncio.get_event_loop()
                    user_info = await loop.run_in_executor(None, self.get_user_info, message.from_user.id)

                    if user_info and user_info.get('agent_name'):
                        keyboard = InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="Line 📞", callback_data="request_line")]
                        ])
                        await message.answer(
                            f"👋 Welcome back, <b>{user_info['agent_name']}</b>!\n\n"
                            f"🗃️ Reference: <code>{user_info['reference']}</code>\n\n"
                            "Press the button below to request a line",
                            parse_mode="HTML",
                            reply_markup=keyboard
                        )
                    else:
                        await state.set_state(UserStates.waiting_for_agent_name)
                        await message.answer("👋 Welcome! Please enter your agent name:")
            else:
                await message.answer("👋 Use /start in private chat!")

        @self.router.message(UserStates.waiting_for_agent_name, F.text)
        async def receive_agent_name(message: Message, state: FSMContext):
            if message.text.startswith('/cancel'):
                await state.clear()
                await message.answer("❌ Cancelled.")
                return

            agent_name = message.text.strip()
            if len(agent_name) < 2 or len(agent_name) > 50:
                await message.answer("❌ Agent name must be 2-50 characters!")
                return

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.save_agent_name, message.from_user.id, agent_name)

            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Line 📞", callback_data="request_line")]
            ])

            await message.answer(
                f"✅ <b>Agent name saved!</b>\n\n"
                f"👤 <b>Agent:</b> {agent_name}\n"
                f"🗃️ Reference: <code>{self.reference}</code>\n\n"
                "Press the button below to request a line",
                parse_mode="HTML",
                reply_markup=keyboard
            )
            await state.clear()

        @self.router.callback_query(F.data.startswith("otp_"))
        async def callback_otp(callback: CallbackQuery):
            user_id = int(callback.data.split("_")[1])

            if callback.from_user.id != user_id:
                await callback.answer("Not for you!", show_alert=True)
                return

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.update_record_status, user_id, "OTP")

            username = callback.from_user.username or callback.from_user.first_name
            user_mention = callback.from_user.mention_html()

            record = await loop.run_in_executor(None, self.get_user_record, user_id)
            user_info = await loop.run_in_executor(None, self.get_user_info, user_id)
            agent_name = user_info['agent_name'] if user_info and user_info.get('agent_name') else "Agent"
            reference = user_info['reference'] if user_info else self.reference

            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Need a Pass ⛹️", callback_data=f"need_pass_{user_id}")],
                [InlineKeyboardButton(text="Finishing 🫡", callback_data=f"finishing_{user_id}")],
                [InlineKeyboardButton(text="Vic Needs Callback ☎️", callback_data=f"vic_callback_{user_id}")],
                [InlineKeyboardButton(text="Call Ended 📵", callback_data=f"call_ended_{user_id}")]
            ])

            await callback.message.edit_text(
                f"{callback.message.text}\n\n✅ Status: OTP 📞\n\nWhat do you need?",
                reply_markup=keyboard
            )

            if record:
                admin_message = f"📲 <b>{user_mention} is on call (OTP)</b>\n\n"
                admin_message += f"👤 <b>Agent:</b> {agent_name}\n"
                admin_message += f"🗃️ Reference: <code>{reference}</code>\n\n"
                admin_message += "<b>🎫Line Details:</b>\n\n"
                if record.get('name'):
                    admin_message += f"👤 Name: {record['name']}\n"
                admin_message += f"📞 Number: <code>{record['number']}</code>\n"
                if record.get('address'):
                    admin_message += f"📍 Address: {record['address']}\n"
                if record.get('email'):
                    admin_message += f"📧 Email: {record['email']}\n"

                try:
                    await self.bot.send_message(chat_id=self.admin_id, text=admin_message, parse_mode="HTML")
                except Exception as e:
                    logging.error(f"Error sending to admin: {e}")

                admins = await loop.run_in_executor(None, self.get_all_admins)
                for admin in admins:
                    if admin['user_id'] != self.admin_id:
                        try:
                            await self.bot.send_message(chat_id=admin['user_id'], text=admin_message, parse_mode="HTML")
                        except Exception as e:
                            logging.error(f"Error sending to admin {admin['user_id']}: {e}")

            try:
                await self.bot.send_message(chat_id=self.group_chat_id, text=f"📲 {user_mention} is on call")
            except Exception as e:
                logging.error(f"Error: {e}")

            await callback.answer()

        # Add ALL your other handlers here (callback_noanswer, cmd_stats, etc.)
        # Just replace BOT_TOKEN, ADMIN_ID, etc. with self.bot_token, self.admin_id, etc.

    async def start(self):
        """Start the bot"""
        try:
            logging.info(f"📦 Initializing bot {self.reference}...")
            self.init_database()

            self.bot = Bot(token=self.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
            self.dp = Dispatcher(storage=MemoryStorage())
            self.dp.include_router(self.router)

            logging.info(f"🤖 Bot {self.reference} started!")
            await self.dp.start_polling(self.bot)

        except Exception as e:
            logging.error(f"❌ Error in bot {self.reference}: {e}")
            raise
