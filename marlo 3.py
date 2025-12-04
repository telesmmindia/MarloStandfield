import asyncio
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, StateFilter
from aiogram.types import Message, BufferedInputFile, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, \
    BotCommand, FSInputFile
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramForbiddenError
import pymysql
from pymysql.cursors import DictCursor
import logging
from contextlib import contextmanager
import io
import csv

logging.basicConfig(level=logging.INFO)

# Configuration
BOT_TOKEN = "8525908374:AAFj61KCJNXypKM5moKxmWF1fhQk0i6UXQk"
ADMIN_ID = 7593649217
GROUP_CHAT_ID = -5004650273
REFERENCE = 'LG206187'


# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '3kkxb7jdfh',
    'database': 'marlo_usa',
    'charset': 'utf8mb4',
    'cursorclass': DictCursor
}


# FSM States
class AdminStates(StatesGroup):
    waiting_for_numbers = State()
    waiting_for_file = State()
    waiting_for_forward = State()
    waiting_for_remove_forward = State()


class UserStates(StatesGroup):
    waiting_for_agent_name = State()  # NEW - Add this
    waiting_for_summary = State()
    waiting_for_callback_summary = State()
    waiting_for_finishing_summary = State()
    waiting_for_call_ended_summary = State()


# Database connection context manager
@contextmanager
def get_db_connection():
    connection = pymysql.connect(**DB_CONFIG)
    try:
        yield connection
        connection.commit()
    except Exception as e:
        connection.rollback()
        logging.error(f"Database error: {e}")
        raise
    finally:
        connection.close()


# Database initialization
def init_database():
    """Initialize database and create tables if they don't exist"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS bot_status (
                   id INT PRIMARY KEY DEFAULT 1,
                   status VARCHAR(50) DEFAULT 'running'
                )"""
            )
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
                    reference VARCHAR(50) DEFAULT '{REFERENCE}',
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
                CREATE TABLE IF NOT EXISTS approved_new_numbers (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    approved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    used BOOLEAN DEFAULT FALSE,
                    INDEX idx_user_id (user_id),
                    INDEX idx_used (used)
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

            logging.info("✅ Database tables initialized successfully")

def save_agent_name(user_id, agent_name):
    """Save or update agent name for user"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""INSERT INTO users (user_id, agent_name, reference) 
                   VALUES (%s, %s, '{REFERENCE}') 
                   ON DUPLICATE KEY UPDATE agent_name = %s""",
                (user_id, agent_name, agent_name)
            )

def get_user_info(user_id):
    """Get agent name and reference for user"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT agent_name, reference FROM users WHERE user_id = %s",
                (user_id,)
            )
            return cursor.fetchone()

def save_no_answer_record(user_id, username, agent_name, reference, number, name, address, email):
    """Save no answer record for export"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """INSERT INTO no_answer_records 
                   (user_id, username, agent_name, reference, number, name, address, email) 
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (user_id, username, agent_name, reference, number, name, address, email)
            )


def is_admin(user_id):
    """Check if user is an admin (master or regular)"""
    if user_id == ADMIN_ID:
        return True
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id FROM admins WHERE user_id = %s", (user_id,))
            return cursor.fetchone() is not None

def is_master_admin(user_id):
    """Check if user is the master admin"""
    return user_id == ADMIN_ID

def add_admin(user_id, username, added_by):
    """Add a new admin"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO admins (user_id, username, added_by) VALUES (%s, %s, %s)",
                (user_id, username, added_by)
            )

def remove_admin(user_id):
    """Remove an admin"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM admins WHERE user_id = %s", (user_id,))
            return cursor.rowcount

def get_all_admins():
    """Get list of all admins"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT user_id, username, added_at FROM admins ORDER BY added_at")
            return cursor.fetchall()


# Database functions
def add_numbers_to_queue(numbers_list):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            query = "INSERT INTO number_queue (number) VALUES (%s)"
            cursor.executemany(query, [(num.strip(),) for num in numbers_list])


def add_records_from_csv(records_list):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            query = """INSERT INTO number_queue (number, name, address, email) 
                       VALUES (%s, %s, %s, %s)"""
            cursor.executemany(query, records_list)

def clear_pending_requests():
    """Delete all pending line requests"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM number_requests WHERE status in ('pending','approved')"
            )
            deleted_count = cursor.rowcount
            conn.commit()
            return deleted_count


def get_user_record(user_id: int):
    with get_db_connection() as conn:
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
            result = cursor.fetchone()
            return result if result else None

def mark_line_completed(user_id):
    """Mark a line as permanently completed"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE number_queue SET is_completed = TRUE WHERE used_by_user_id = %s AND is_used = TRUE",
                (user_id,)
            )
            cursor.execute(
                "DELETE FROM number_requests WHERE user_id = %s AND status IN ('pending', 'approved')",
                (user_id,)
            )


def get_next_number(user_id: int, username: str = None, force_new: bool = False):
    """
    Get next available number for user
    force_new: If True, ignore existing assignment check (for "request another line")
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            # Only check for existing if force_new is False
            if not force_new:
                cursor.execute(
                    "SELECT number FROM number_queue WHERE used_by_user_id = %s AND is_used = TRUE AND is_completed = FALSE LIMIT 1",
                    (user_id,)
                )
                existing = cursor.fetchone()

                if existing:
                    return None

            # Get first unused number - EXCLUDE COMPLETED
            cursor.execute(
                "SELECT id, number, name, address, email FROM number_queue WHERE is_used = FALSE AND is_completed = FALSE ORDER BY id LIMIT 1"
            )
            result = cursor.fetchone()

            if result:
                record_id = result['id']
                cursor.execute(
                    """UPDATE number_queue 
                       SET is_used = TRUE, 
                           used_by_user_id = %s, 
                           used_by_username = %s,
                           used_at = NOW()
                       WHERE id = %s""",
                    (user_id, username, record_id)
                )
                return result
            return None




def update_record_status(user_id: int, status: str):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE number_queue SET status = %s WHERE used_by_user_id = %s",
                (status, user_id)
            )


def update_record_summary(user_id: int, summary: str):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """UPDATE number_queue 
                   SET call_summary = %s, summary_submitted_at = NOW() 
                   WHERE used_by_user_id = %s""",
                (summary, user_id)
            )


def create_line_request(user_id: int, username: str):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """INSERT INTO number_requests 
                   (user_id, username, reason, status) 
                   VALUES (%s, %s, %s, 'pending')""",
                (user_id, username, "line_request")
            )
            return cursor.lastrowid


def get_request_by_id(request_id: int):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT * FROM number_requests WHERE id = %s",
                (request_id,)
            )
            return cursor.fetchone()


def update_request_status(request_id: int, status: str):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """UPDATE number_requests 
                   SET status = %s, processed_at = NOW() 
                   WHERE id = %s""",
                (status, request_id)
            )

def export_no_answer_records():
    """Export all no answer records"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT user_id, username, agent_name, reference, number, name, 
                       address, email, created_at 
                FROM no_answer_records 
                ORDER BY created_at DESC
            """)
            results = cursor.fetchall()

            if not results:
                return None, 0

            output = io.StringIO()
            output.write("User ID,Username,Agent Name,Reference,Number,Name,Address,Email,Date\n")
            for row in results:
                name = row['name'] if row['name'] else "-"
                address = row['address'].replace('"', '""') if row['address'] else "-"
                email = row['email'] if row['email'] else "-"
                output.write(
                    f"{row['user_id']},{row['username']},{row['agent_name']},{row['reference']},{row['number']},{name},\"{address}\",{email},{row['created_at']}\n"
                )

            # Delete exported records
            cursor.execute("DELETE FROM no_answer_records")
            deleted_count = cursor.rowcount

            return output.getvalue(), deleted_count

def get_queue_stats():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) as count FROM number_queue WHERE is_used = FALSE"
            )
            remaining = cursor.fetchone()['count']

            cursor.execute(
                "SELECT COUNT(*) as count FROM number_queue WHERE is_used = TRUE"
            )
            used = cursor.fetchone()['count']

            return remaining, used


def reset_queue():
    """Reset only INCOMPLETE lines (don't reset completed ones)"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """UPDATE number_queue 
                   SET is_used = FALSE, 
                       used_by_user_id = NULL, 
                       used_by_username = NULL,
                       used_at = NULL,
                       status = NULL,
                       call_summary = NULL
                   WHERE is_completed = FALSE"""  # Only reset incomplete
            )
            return cursor.rowcount



def clear_queue():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM number_queue")
            cursor.execute("DELETE FROM number_requests")
            return cursor.rowcount


def export_used_numbers():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            # Export COMPLETED lines
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
                status = row['status'] if row['status'] else "-"
                summary = row['call_summary'].replace('"', '""') if row['call_summary'] else "-"
                summary_at = row['summary_submitted_at'] if row['summary_submitted_at'] else "-"
                output.write(
                    f"{row['number']},{row['name']},\"{row['address']}\",{row['email']},{row['used_by_username']},{row['used_by_user_id']},{status},\"{summary}\",{row['used_at']},{summary_at}\n")

            # Delete COMPLETED lines
            cursor.execute("DELETE FROM number_queue WHERE is_completed = TRUE")
            deleted_count = cursor.rowcount

            return output.getvalue(), deleted_count


def export_unused_numbers():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            # Export UNUSED lines (remaining in queue)
            cursor.execute("""
                SELECT number, name, address, email 
                FROM number_queue 
                WHERE is_used = FALSE AND is_completed = FALSE
                ORDER BY id
            """)
            results = cursor.fetchall()

            if not results:
                return None, 0

            output = io.StringIO()
            output.write("Number,Name,Address,Email\n")
            for row in results:
                name = row['name'] if row['name'] else "-"
                address = row['address'] if row['address'] else "-"
                email = row['email'] if row['email'] else "-"
                output.write(f"{row['number']},{name},\"{address}\",{email}\n")

            return output.getvalue(), 0



def export_all_numbers():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT number, name, address, email, is_used, used_by_username, 
                       used_by_user_id, status, call_summary, used_at, added_at 
                FROM number_queue 
                ORDER BY id
            """)
            results = cursor.fetchall()

            if not results:
                return None, 0

            output = io.StringIO()
            output.write("Number,Name,Address,Email,Used,Username,User ID,Status,Summary,Used At,Added At\n")
            for row in results:
                used = "Yes" if row['is_used'] else "No"
                name = row['name'] if row['name'] else "-"
                address = row['address'] if row['address'] else "-"
                email = row['email'] if row['email'] else "-"
                username = row['used_by_username'] if row['used_by_username'] else "-"
                user_id = row['used_by_user_id'] if row['used_by_user_id'] else "-"
                status = row['status'] if row['status'] else "-"
                summary = row['call_summary'].replace('"', '""') if row['call_summary'] else "-"
                used_at = row['used_at'] if row['used_at'] else "-"
                output.write(
                    f"{row['number']},{name},\"{address}\",{email},{used},{username},{user_id},{status},\"{summary}\",{used_at},{row['added_at']}\n")

            cursor.execute("DELETE FROM number_queue")
            deleted_count = cursor.rowcount

            return output.getvalue(), deleted_count


# Router setup
router = Router()


async def set_bot_commands(bot: Bot):
    """Set up bot command menus for different user types"""
    from aiogram.types import BotCommandScopeChat

    # Commands for ADMIN
    admin_commands = [
        BotCommand(command="start", description="Start the bot"),
        BotCommand(command="line", description="Request a new line"),
        BotCommand(command="add", description="Add numbers manually"),
        BotCommand(command="done", description="Confirm numbers addition"),
        BotCommand(command="upload", description="Upload CSV/TXT file"),
        BotCommand(command="addadmin", description="Add new admin (Master only)"),
        BotCommand(command="removeadmin", description="Remove admin (Master only)"),
        BotCommand(command="listadmins", description="List all admins"),
        BotCommand(command="stats", description="View queue statistics"),
        BotCommand(command="reset", description="Reset all numbers"),
        BotCommand(command="clear", description="Delete all numbers"),
        BotCommand(command="clearrequests", description="Clear pending requests"),
        BotCommand(command="export_used", description="Export used numbers"),
        BotCommand(command="export_unused", description="Export unused numbers"),
        BotCommand(command="export_all", description="Export all data"),
        BotCommand(command="export_no_answer", description="Export no answer records"),
        BotCommand(command="export_callbacks", description="Export callback records"),
        BotCommand(command="stop", description="Stop the bot"),
    ]

    # Commands for REGULAR USERS
    user_commands = [
        BotCommand(command="start", description="Start the bot"),
    ]

    # Set commands for master admin
    await bot.set_my_commands(
        admin_commands,
        scope=BotCommandScopeChat(chat_id=ADMIN_ID)
    )

    # Set commands for all other admins
    try:
        admins = get_all_admins()
        for admin in admins:
            if admin['user_id'] != ADMIN_ID:
                try:
                    await bot.set_my_commands(
                        admin_commands,
                        scope=BotCommandScopeChat(chat_id=admin['user_id'])
                    )
                except Exception as e:
                    logging.error(f"Failed to set commands for admin {admin['user_id']}: {e}")
    except Exception as e:
        logging.error(f"Error getting admins: {e}")

    # Set default commands for everyone else
    await bot.set_my_commands(user_commands)

    logging.info("✅ Bot commands set!")


@router.message(Command("listadmins"), F.from_user.id == ADMIN_ID)
async def cmd_list_admins(message: Message):
    """List all admins"""
    loop = asyncio.get_event_loop()
    admins = await loop.run_in_executor(None, get_all_admins)

    text = "<b>👑 Admin List</b>\n\n"
    text += f"<b>Master Admin:</b>\n• ID: <code>{ADMIN_ID}</code>\n\n"

    if admins:
        text += "<b>Other Admins:</b>\n"
        for admin in admins:
            if admin['user_id'] != ADMIN_ID:
                text += f"• {admin['username']} (ID: <code>{admin['user_id']}</code>)\n"
    else:
        text += "<i>No other admins</i>"

    await message.answer(text, parse_mode=ParseMode.HTML)


@router.message(Command("addadmin"), F.from_user.id == ADMIN_ID)
async def cmd_add_admin(message: Message, state: FSMContext, bot: Bot):
    """Master admin adds new admin - works in group (reply) or private (forward)"""

    # METHOD 1: Reply to someone's message in group/private
    if message.reply_to_message:
        new_admin_id = message.reply_to_message.from_user.id
        new_admin_username = message.reply_to_message.from_user.username or message.reply_to_message.from_user.first_name

        if new_admin_id == ADMIN_ID:
            await message.answer("❌ This user is already the master admin.")
            return

        loop = asyncio.get_event_loop()

        # Check if already admin
        if await loop.run_in_executor(None, is_admin, new_admin_id):
            await message.answer(f"ℹ️ {new_admin_username} is already an admin.")
            return

        # Add the admin
        try:
            await loop.run_in_executor(None, add_admin, new_admin_id, new_admin_username, ADMIN_ID)

            # Set admin commands for the new admin
            from aiogram.types import BotCommandScopeChat
            admin_commands = [
                BotCommand(command="start", description="Start the bot"),
                BotCommand(command="line", description="Request a new line"),
                BotCommand(command="add", description="Add numbers manually"),
                BotCommand(command="done", description="Confirm numbers addition"),
                BotCommand(command="upload", description="Upload CSV/TXT file"),
                BotCommand(command="addadmin", description="Add new admin (Master only)"),
                BotCommand(command="removeadmin", description="Remove admin (Master only)"),
                BotCommand(command="listadmins", description="List all admins"),
                BotCommand(command="stats", description="View queue statistics"),
                BotCommand(command="reset", description="Reset all numbers"),
                BotCommand(command="clear", description="Delete all numbers"),
                BotCommand(command="clearrequests", description="Clear pending requests"),
                BotCommand(command="export_used", description="Export used numbers"),
                BotCommand(command="export_unused", description="Export unused numbers"),
                BotCommand(command="export_all", description="Export all data"),
                BotCommand(command="export_no_answer", description="Export no answer records"),
                BotCommand(command="export_callbacks", description="Export callback records"),
                BotCommand(command="stop", description="Stop the bot"),
            ]

            try:
                await bot.set_my_commands(
                    admin_commands,
                    scope=BotCommandScopeChat(chat_id=new_admin_id)
                )
            except:
                pass

            await message.answer(
                f"✅ <b>Admin Added!</b>\n\n"
                f"User: {new_admin_username}\n"
                f"ID: <code>{new_admin_id}</code>\n\n"
                f"They can now approve/decline line requests.",
                parse_mode=ParseMode.HTML
            )

            # Notify the new admin
            try:
                await bot.send_message(
                    chat_id=new_admin_id,
                    text="🎉 You've been promoted to admin! You can now approve line requests.",
                    parse_mode=ParseMode.HTML
                )
            except:
                pass

        except Exception as e:
            await message.answer(f"❌ Error adding admin: {e}")

        return

    # METHOD 2: In private chat - ask for forwarded message
    if message.chat.type == "private":
        await state.set_state(AdminStates.waiting_for_forward)
        await message.answer(
            "<b>👑 Add Admin</b>\n\n"
            "Forward me any message from the user you want to make admin.\n\n"
            "Send /cancel to cancel.",
            parse_mode=ParseMode.HTML
        )
    else:
        await message.answer("❌ Reply to a user's message or use this in private chat.")


@router.message(AdminStates.waiting_for_forward, F.forward_from)
async def receive_forward_add_admin(message: Message, state: FSMContext):
    """Handle forwarded message to add admin"""
    new_admin_id = message.forward_from.id
    new_admin_username = message.forward_from.username or message.forward_from.first_name

    if new_admin_id == ADMIN_ID:
        await message.answer("❌ This user is already the master admin.")
        await state.clear()
        return

    loop = asyncio.get_event_loop()

    # Check if already admin
    if await loop.run_in_executor(None, is_admin, new_admin_id):
        await message.answer(f"ℹ️ {new_admin_username} is already an admin.")
        await state.clear()
        return

    # Add the admin
    try:
        await loop.run_in_executor(None, add_admin, new_admin_id, new_admin_username, ADMIN_ID)
        await message.answer(
            f"✅ <b>Admin Added!</b>\n\n"
            f"User: {new_admin_username}\n"
            f"ID: <code>{new_admin_id}</code>\n\n"
            f"They can now approve/decline line requests.",
            parse_mode=ParseMode.HTML
        )

        # Notify the new admin
        try:
            await message.bot.send_message(
                chat_id=new_admin_id,
                text="🎉 You've been promoted to admin! You can now approve line requests.",
                parse_mode=ParseMode.HTML
            )
        except:
            pass

    except Exception as e:
        await message.answer(f"❌ Error adding admin: {e}")

    await state.clear()


@router.message(AdminStates.waiting_for_forward, ~F.forward_from)
async def receive_non_forward_add_admin(message: Message, state: FSMContext):
    """Handle non-forwarded message during add admin flow"""
    if message.text and message.text.startswith('/cancel'):
        await state.clear()
        await message.answer("❌ Cancelled.")
        return

    await message.answer(
        "❌ Please forward a message from the user.\n\n"
        "The message must show 'Forwarded from [username]' at the top.\n\n"
        "Send /cancel to cancel."
    )


@router.message(Command("removeadmin"), F.from_user.id == ADMIN_ID)
async def cmd_remove_admin(message: Message, state: FSMContext, bot: Bot):
    """Master admin removes admin - works in group (reply) or private (forward)"""

    # METHOD 1: Reply to someone's message in group/private
    if message.reply_to_message:
        admin_to_remove = message.reply_to_message.from_user.id
        admin_username = message.reply_to_message.from_user.username or message.reply_to_message.from_user.first_name

        if admin_to_remove == ADMIN_ID:
            await message.answer("❌ Cannot remove master admin.")
            return

        loop = asyncio.get_event_loop()
        count = await loop.run_in_executor(None, remove_admin, admin_to_remove)

        if count > 0:
            # Reset to user commands for removed admin
            from aiogram.types import BotCommandScopeChat
            user_commands = [
                BotCommand(command="start", description="Start the bot"),
            ]

            try:
                await bot.set_my_commands(
                    user_commands,
                    scope=BotCommandScopeChat(chat_id=admin_to_remove)
                )
            except:
                pass

            await message.answer(
                f"✅ <b>Admin Removed!</b>\n\n"
                f"User: {admin_username}\n"
                f"ID: <code>{admin_to_remove}</code>\n\n"
                f"They can no longer approve line requests.",
                parse_mode=ParseMode.HTML
            )

            # Notify the removed admin
            try:
                await bot.send_message(
                    chat_id=admin_to_remove,
                    text="ℹ️ Your admin privileges have been removed.",
                    parse_mode=ParseMode.HTML
                )
            except:
                pass
        else:
            await message.answer(f"❌ {admin_username} is not an admin.")

        return

    # METHOD 2: In private chat - ask for forwarded message
    if message.chat.type == "private":
        await state.set_state(AdminStates.waiting_for_remove_forward)
        await message.answer(
            "<b>👑 Remove Admin</b>\n\n"
            "Forward me any message from the admin you want to remove.\n\n"
            "Send /cancel to cancel.",
            parse_mode=ParseMode.HTML
        )
    else:
        await message.answer("❌ Reply to an admin's message or use this in private chat.")


@router.message(AdminStates.waiting_for_remove_forward, F.forward_from)
async def receive_forward_remove_admin(message: Message, state: FSMContext):
    """Handle forwarded message to remove admin"""
    admin_to_remove = message.forward_from.id
    admin_username = message.forward_from.username or message.forward_from.first_name

    if admin_to_remove == ADMIN_ID:
        await message.answer("❌ Cannot remove master admin.")
        await state.clear()
        return

    loop = asyncio.get_event_loop()
    count = await loop.run_in_executor(None, remove_admin, admin_to_remove)

    if count > 0:
        await message.answer(
            f"✅ <b>Admin Removed!</b>\n\n"
            f"User: {admin_username}\n"
            f"ID: <code>{admin_to_remove}</code>\n\n"
            f"They can no longer approve line requests.",
            parse_mode=ParseMode.HTML
        )

        # Notify the removed admin
        try:
            await message.bot.send_message(
                chat_id=admin_to_remove,
                text="ℹ️ Your admin privileges have been removed.",
                parse_mode=ParseMode.HTML
            )
        except:
            pass
    else:
        await message.answer(f"❌ {admin_username} is not an admin.")

    await state.clear()


@router.message(AdminStates.waiting_for_remove_forward, ~F.forward_from)
async def receive_non_forward_remove_admin(message: Message, state: FSMContext):
    """Handle non-forwarded message during remove admin flow"""
    if message.text and message.text.startswith('/cancel'):
        await state.clear()
        await message.answer("❌ Cancelled.")
        return

    await message.answer(
        "❌ Please forward a message from the user.\n\n"
        "The message must show 'Forwarded from [username]' at the top.\n\n"
        "Send /cancel to cancel."
    )


@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    if message.chat.type == "private":
        if message.from_user.id == ADMIN_ID:
            status = get_bot_status()
            if status == "stopped":
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, set_bot_status, "running")
                await message.answer("▶️ <b>Bot restarted!</b>\n\nUsers can now request lines.", parse_mode="HTML")
            else:
                await message.answer(
                    "🤖 <b>Admin Commands:</b>\n\n"
                    "<b>Management:</b>\n"
                    "/add - Add numbers manually\n"
                    "/upload - Upload CSV/TXT file\n"
                    "/stats - View queue statistics\n"
                    "/reset - Reset all numbers\n"
                    "/clear - Delete all numbers\n\n"
                    "<b>Export Data:</b>\n"
                    "/export_used - Download used numbers\n"
                    "/export_unused - Download remaining\n"
                    "/export_all - Download complete report",
                    parse_mode="HTML"
                )
        else:
            try:
                member = await message.bot.get_chat_member(GROUP_CHAT_ID, message.from_user.id)
                if member.status == "kicked":
                    await message.answer("⚠️ You're blocked from the group!")
                    return
                if member.status == 'left':
                    await message.answer("⚠️ You must be a member of the group!")
                    return
            except Exception as e:
                await message.answer("⚠️ You must be a member of the group!")
                return

            # Check if user already has agent name
            loop = asyncio.get_event_loop()
            user_info = await loop.run_in_executor(None, get_user_info, message.from_user.id)

            if user_info and user_info.get('agent_name'):
                # User already set agent name - show line button
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
                # First time user - ask for agent name
                await state.set_state(UserStates.waiting_for_agent_name)
                await message.answer(
                    "👋 Welcome! Please enter your agent name:\n\n"
                    "Send /cancel to abort.",
                    parse_mode="HTML"
                )
    else:
        await message.answer("👋 Use /start in private chat!")


@router.message(UserStates.waiting_for_agent_name, F.text)
async def receive_agent_name(message: Message, state: FSMContext):
    if message.text.startswith('/cancel'):
        await state.clear()
        await message.answer("❌ Cancelled.")
        return

    user_id = message.from_user.id
    agent_name = message.text.strip()

    if len(agent_name) < 2 or len(agent_name) > 50:
        await message.answer("❌ Agent name must be 2-50 characters!")
        return

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, save_agent_name, user_id, agent_name)

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Line 📞", callback_data="request_line")]
    ])

    await message.answer(
        f"✅ <b>Agent name saved!</b>\n\n"
        f"👤 <b>Agent:</b> {agent_name}\n"
        f"🗃️ Reference: <code>{REFERENCE}</code>\n\n"
        "Press the button below to request a line",
        parse_mode="HTML",
        reply_markup=keyboard
    )

    await state.clear()


# Add this to your database functions
def set_bot_status(status: str):
    """Set bot status (running/stopped)"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS bot_status (
                   id INT PRIMARY KEY DEFAULT 1,
                   status VARCHAR(50) DEFAULT 'running'
                )"""
            )
            cursor.execute(
                "INSERT INTO bot_status (id, status) VALUES (1, %s) ON DUPLICATE KEY UPDATE status = %s",
                (status, status)
            )


def get_bot_status():
    """Get bot status"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT status FROM bot_status WHERE id = 1")
            result = cursor.fetchone()
            return result['status'] if result else 'running'


# Add these commands to your router
@router.message(Command("stop"), F.from_user.id == ADMIN_ID)
async def cmd_stop(message: Message):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, set_bot_status, "stopped")
    await message.answer("⏸️ <b>Bot stopped!</b>\n\nUsers can't request lines now.", parse_mode="HTML")


@router.message(Command("clearrequests"), F.from_user.id == ADMIN_ID)
async def cmd_clear_requests(message: Message):
    """Clear all pending line requests"""
    loop = asyncio.get_event_loop()
    deleted_count = await loop.run_in_executor(None, clear_pending_requests)

    await message.answer(
        f"<b>Pending Requests Cleared</b>\n"
        f"Deleted: <b>{deleted_count}</b> pending request(s)",
        parse_mode=ParseMode.HTML
    )


# /line command - kept for users who prefer command
@router.message(Command("hcfyjh87hv"))
async def cmd_line(message: Message, bot: Bot):
    if message.chat.type not in ["group", "supergroup"]:
        await message.reply("⚠️ Use this in the group!")
        return

    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.first_name
    user_mention = message.from_user.mention_html()

    loop = asyncio.get_event_loop()

    # CHECK: Does user already have a line assigned?
    existing_record = await loop.run_in_executor(None, get_user_record, user_id)

    if existing_record and existing_record.get('is_used'):
        await message.reply(
            f"⚠️ {user_mention}, you already have a line assigned!\n\n"
            "Once you're done, you can request another line.",
            parse_mode="HTML"
        )
        return

    # Create new line request
    request_id = await loop.run_in_executor(None, create_line_request, user_id, username)

    # Notify admin
    admin_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Approve", callback_data=f"approve_line_{request_id}"),
            InlineKeyboardButton(text="❌ Decline", callback_data=f"decline_line_{request_id}")
        ]
    ])

    await bot.send_message(
        chat_id=ADMIN_ID,
        text=f"🔔 <b>New Line Request</b>\n\n👤 <b>User:</b> {username} (ID: {user_id})",
        parse_mode="HTML",
        reply_markup=admin_keyboard
    )

    # Notify group
    await message.reply(f"📝 {user_mention} has requested a new line", parse_mode="HTML")


# New member welcome
@router.message(F.new_chat_members)
async def new_member_welcome(message: Message):
    if message.chat.type not in ["group", "supergroup"]:
        return

    for new_member in message.new_chat_members:
        if new_member.is_bot:
            continue

        user_mention = new_member.mention_html()

        await message.answer(
            f"👋 {user_mention} welcome to the group chat, message me in private to start",
            parse_mode="HTML"
        )


# Upload CSV file
@router.message(Command("upload"), F.chat.type == "private", F.from_user.id == ADMIN_ID)
async def cmd_upload(message: Message, state: FSMContext):
    await message.answer(
        "📤 <b>Upload CSV/TXT File</b>\n\n"
        "Send me a CSV or TXT file with:\n"
        "<code>Number,Name,Address,Email</code>",
        parse_mode="HTML"
    )
    await state.set_state(AdminStates.waiting_for_file)


def detect_column_mapping(header_row):
    """Auto-detect which column is which"""
    mapping = {'number': None, 'name': None, 'address': None, 'email': None}

    for idx, col in enumerate(header_row):
        col_lower = col.lower().strip()

        # Detect number column
        if 'number' in col_lower or 'phone' in col_lower or 'tel' in col_lower:
            mapping['number'] = idx
        # Detect name column
        elif 'name' in col_lower or 'full' in col_lower or 'fname' in col_lower:
            mapping['name'] = idx
        # Detect address column
        elif 'address' in col_lower or 'location' in col_lower or 'addr' in col_lower:
            mapping['address'] = idx
        # Detect email column
        elif 'email' in col_lower or 'mail' in col_lower or 'e-mail' in col_lower:
            mapping['email'] = idx

    return mapping


@router.message(AdminStates.waiting_for_file, F.document)
async def handle_file_upload(message: Message, state: FSMContext, bot: Bot):
    document = message.document

    if not (document.file_name.endswith('.csv') or document.file_name.endswith('.txt')):
        await message.answer("❌ Please send a CSV or TXT file only!")
        return

    try:
        file = await bot.get_file(document.file_id)
        file_content = await bot.download_file(file.file_path)

        content = file_content.read().decode('utf-8')
        csv_reader = csv.reader(io.StringIO(content))

        # Read header and detect columns
        header = next(csv_reader)
        mapping = detect_column_mapping(header)

        # Validate we found number column
        if mapping['number'] is None:
            await message.answer("❌ Could not detect 'Number' column!")
            await state.clear()
            return

        records = []
        for row in csv_reader:
            if len(row) < 2:  # Skip empty rows
                continue

            # Extract based on detected mapping
            number = row[mapping['number']].strip() if mapping['number'] is not None else None
            name = row[mapping['name']].strip() if mapping['name'] is not None and len(row) > mapping['name'] else None
            address = row[mapping['address']].strip() if mapping['address'] is not None and len(row) > mapping[
                'address'] else None
            email = row[mapping['email']].strip() if mapping['email'] is not None and len(row) > mapping[
                'email'] else None

            if number:  # Only add if number exists
                records.append((number, name, address, email))

        if records:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, add_records_from_csv, records)

            # Show detected mapping
            cols_found = [f"{k.title()}: Col {v + 1}" for k, v in mapping.items() if v is not None]
            await message.answer(
                f"✅ <b>Imported {len(records)} records!</b>\n\n"
                f"📋 Detected columns:\n• " + "\n• ".join(cols_found),
                parse_mode="HTML"
            )
        else:
            await message.answer("❌ No valid records found!")

    except Exception as e:
        logging.error(f"Error: {e}")
        await message.answer(f"❌ Error: {str(e)}")

    await state.clear()


@router.message(AdminStates.waiting_for_file, Command("cancel"))
async def cancel_upload(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Upload cancelled.")


# Add numbers command
def detect_column_mapping_text(header_row):
    """Auto-detect which column is which"""
    mapping = {'number': None, 'name': None, 'address': None, 'email': None}

    for idx, col in enumerate(header_row):
        col_lower = col.lower().strip()

        if 'number' in col_lower or 'phone' in col_lower or 'tel' in col_lower:
            mapping['number'] = idx
        elif 'name' in col_lower or 'full' in col_lower or 'fname' in col_lower:
            mapping['name'] = idx
        elif 'address' in col_lower or 'location' in col_lower or 'addr' in col_lower:
            mapping['address'] = idx
        elif 'email' in col_lower or 'mail' in col_lower or 'e-mail' in col_lower:
            mapping['email'] = idx

    return mapping


@router.message(Command("add"), F.chat.type == "private", F.from_user.id == ADMIN_ID )
async def cmd_add(message: Message, state: FSMContext):
    await message.answer(
        "📝 Send data in CSV format (Number,Name,Address,Email) or just numbers.\n"
        "Auto-detects column order!\n\n"
        "Send /done when finished.",
        parse_mode="HTML"
    )
    await state.set_state(AdminStates.waiting_for_numbers)


@router.message(AdminStates.waiting_for_numbers, Command("cancel"))
async def cancel_adding(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Cancelled.")


@router.message(AdminStates.waiting_for_numbers, Command("done"))
async def finish_adding(message: Message, state: FSMContext):
    data = await state.get_data()
    numbers = data.get('numbers', [])

    if numbers:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, add_records_from_csv, numbers)

        # Show summary
        mapping = data.get('mapping', {})
        cols_found = [f"{k.title()}: Col {v + 1}" for k, v in mapping.items() if v is not None]

        await message.answer(
            f"✅ <b>Added {len(numbers)} records!</b>\n\n"
            f"📋 Detected columns:\n• " + "\n• ".join(cols_found) if cols_found else "✅ Added!",
            parse_mode="HTML"
        )
    else:
        await message.answer("No records added.")

    await state.clear()


@router.message(AdminStates.waiting_for_numbers)
async def receive_numbers(message: Message, state: FSMContext):
    data = await state.get_data()
    numbers = data.get('numbers', [])

    # Parse as CSV with proper quote handling
    csv_reader = csv.reader(io.StringIO(message.text))

    first_row = True
    mapping = None

    for row in csv_reader:
        if not row or len(row) < 1:
            continue

        # Auto-detect on first row
        if first_row:
            # Check if it's a header by looking for keywords
            is_header = any(keyword in row[0].lower() for keyword in
                            ['number', 'phone', 'tel', 'name', 'address', 'email', 'mail'])

            if is_header:
                mapping = detect_column_mapping_text(row)
                first_row = False
                continue
            else:
                # No header, assume: Number,Name,Address,Email order
                mapping = {'number': 0, 'name': 1, 'address': 2, 'email': 3}
                first_row = False

        # Extract based on detected mapping
        number = row[mapping['number']].strip() if mapping['number'] is not None and len(row) > mapping[
            'number'] else None
        name = row[mapping['name']].strip() if mapping['name'] is not None and len(row) > mapping['name'] else None
        address = row[mapping['address']].strip() if mapping['address'] is not None and len(row) > mapping[
            'address'] else None
        email = row[mapping['email']].strip() if mapping['email'] is not None and len(row) > mapping['email'] else None

        if number:  # Only add if number exists
            numbers.append((number, name, address, email))

    await state.update_data(numbers=numbers, mapping=mapping)
    await message.answer(f"✅ Parsed {len(numbers)} records correctly!")


# Stats command
@router.message(Command("stats"), F.from_user.id == ADMIN_ID)
async def cmd_stats(message: Message):
    loop = asyncio.get_event_loop()
    remaining, used = await loop.run_in_executor(None, get_queue_stats)
    await message.answer(
        f"📊 <b>Queue Stats:</b>\n\n"
        f"Remaining: {remaining}\n"
        f"Used: {used}\n"
        f"Total: {remaining + used}",
        parse_mode="HTML"
    )


# Reset queue
@router.message(Command("reset"), F.chat.type == "private", F.from_user.id == ADMIN_ID)
async def cmd_reset(message: Message):
    loop = asyncio.get_event_loop()
    count = await loop.run_in_executor(None, reset_queue)
    await message.answer(f"🔄 Reset {count} numbers.")


# Clear queue
@router.message(Command("clear"), F.chat.type == "private", F.from_user.id == ADMIN_ID)
async def cmd_clear(message: Message):
    loop = asyncio.get_event_loop()
    count = await loop.run_in_executor(None, clear_queue)
    await message.answer(f"🗑️ Deleted {count} numbers.")


# Export commands
@router.message(Command("export_used"), F.from_user.id == ADMIN_ID)
async def cmd_export_used(message: Message):
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, export_used_numbers)

    if result[0]:
        csv_content, deleted_count = result
        file_bytes = csv_content.encode('utf-8')
        file = BufferedInputFile(file_bytes, filename="used_numbers.csv")
        await message.answer_document(file, caption=f"📊 Report\n✅ {deleted_count} deleted")
    else:
        await message.answer("No used numbers found.")


@router.message(Command("export_unused"), F.from_user.id == ADMIN_ID)
async def cmd_export_unused(message: Message):
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, export_unused_numbers)

    if result[0]:
        csv_content, deleted_count = result
        file_bytes = csv_content.encode('utf-8')
        file = BufferedInputFile(file_bytes, filename="unused_numbers.csv")
        await message.answer_document(file, caption=f"📝 Report\n✅ {deleted_count} deleted")
    else:
        await message.answer("No unused numbers found.")


@router.message(Command("export_all"), F.from_user.id == ADMIN_ID)
async def cmd_export_all(message: Message):
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, export_all_numbers)

    if result[0]:
        csv_content, deleted_count = result
        file_bytes = csv_content.encode('utf-8')
        file = BufferedInputFile(file_bytes, filename="all_numbers.csv")
        await message.answer_document(file, caption=f"📋 Report\n✅ {deleted_count} deleted")
    else:
        await message.answer("No numbers found.")

@router.message(Command("export_no_answer"), F.from_user.id == ADMIN_ID)
async def cmd_export_no_answer(message: Message):
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, export_no_answer_records)

    if result[0]:
        csv_content, deleted_count = result
        file_bytes = csv_content.encode('utf-8')
        file = BufferedInputFile(file_bytes, filename="no_answer_records.csv")
        await message.answer_document(file, caption=f"❌ No Answer Records\n✅ {deleted_count} records exported")
    else:
        await message.answer("No 'No Answer' records found.")

async def send_request_to_all_admins(bot: Bot, request_id: int, user_mention: str, username: str):
    """Send line request to all admins"""
    loop = asyncio.get_event_loop()
    admins = await loop.run_in_executor(None, get_all_admins)

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Approve", callback_data=f"approve_line_{request_id}"),
            InlineKeyboardButton(text="❌ Decline", callback_data=f"decline_line_{request_id}")
        ]
    ])

    # Send to master admin
    try:
        await bot.send_message(
            chat_id=ADMIN_ID,
            text=f"📞 <b>Line Request</b>\n\nUser: {user_mention}",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
    except:
        pass

    # Send to all other admins
    for admin in admins:
        if admin['user_id'] != ADMIN_ID:
            try:
                await bot.send_message(
                    chat_id=admin['user_id'],
                    text=f"📞 <b>Line Request</b>\n\nUser: {user_mention}",
                    reply_markup=keyboard,
                    parse_mode="HTML"
                )
            except Exception as e:
                logging.error(f"Failed to send to admin {admin['user_id']}: {e}")



@router.callback_query(F.data == "request_line")
async def callback_request_line(callback: CallbackQuery, state: FSMContext, bot: Bot):
    user_id = callback.from_user.id
    username = callback.from_user.username or callback.from_user.first_name

    # 1) CHECK: in group?
    try:
        member = await bot.get_chat_member(GROUP_CHAT_ID, user_id)
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

    # 2) CHECK: bot running?
    status = get_bot_status()
    if status == "stopped":
        await callback.message.answer("⏸️ Bot is currently stopped.", parse_mode="HTML")
        await callback.answer()
        return

    loop = asyncio.get_event_loop()

    # 3) CHECK: user already has active line?
    def has_active_line(uid: int) -> bool:
        with get_db_connection() as conn:
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

    already_has_line = await loop.run_in_executor(None, has_active_line, user_id)
    if already_has_line:
        await callback.message.answer(
            "❌ You already have an active line! Use it first."
        )
        await callback.answer()
        return

    # 4) DIRECTLY assign next number (no number_requests / no approval)
    record = await loop.run_in_executor(
        None, get_next_number, user_id, username, False
    )

    if not record:
        await callback.message.answer("❌ No numbers available in queue right now.")
        await callback.answer()
        return

    # 5) Get agent name + reference
    user_info = await loop.run_in_executor(None, get_user_info, user_id)
    agent_name = (
        user_info["agent_name"]
        if user_info and user_info.get("agent_name")
        else "Agent"
    )
    reference = user_info["reference"] if user_info else REFERENCE

    # 6) Build DM text properly
    dm_message = ""
    dm_message += f"👤 <b>Agent:</b> {agent_name}\n"
    dm_message += f"🗃️ Reference: <code>{reference}</code>\n\n"
    dm_message += "🎫 <b>Your Line:</b>\n\n"

    if record.get("name"):
        dm_message += f"👤 Name: {record['name']}\n"
    dm_message += f"📞 Number: <code>{record['number']}</code>\n"
    if record.get("address"):
        dm_message += f"📍 Address: {record['address']}\n"
    if record.get("email"):
        dm_message += f"📧 Email: {record['email']}\n"

    # 7) OTP / No Answer buttons
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="OTP 📞", callback_data=f"otp_{user_id}"
                ),
                InlineKeyboardButton(
                    text="No Answer ❌", callback_data=f"noanswer_{user_id}"
                ),
            ]
        ]
    )

    try:
        # DM to user
        await bot.send_message(
            chat_id=user_id,
            text=dm_message,
            parse_mode="HTML",
            reply_markup=keyboard,
        )

        # Notify group
        user_mention = f"@{username}" if username else f"User {user_id}"
        await bot.send_message(
            chat_id=GROUP_CHAT_ID,
            text=f"✅ {user_mention} line has been sent in private",
            parse_mode="HTML",
        )

        logging.info(
            f"User {username} ({user_id}) received line: {record['number']}"
        )
    except TelegramForbiddenError:
        await bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                f"⚠️ Can't send DM to {username}. "
                f"They need to start the bot first."
            ),
            parse_mode="HTML",
        )
    except Exception as e:
        logging.error(f"Error sending line: {e}")

    await callback.answer("✅ Line assigned and sent!")


# DECLINE LINE
@router.callback_query(F.data.startswith("decline_line_"))
async def callback_decline_line(callback: CallbackQuery, bot: Bot):
    loop = asyncio.get_event_loop()

    # Check if user is admin
    if not await loop.run_in_executor(None, is_admin, callback.from_user.id):
        await callback.answer("❌ Admin only!", show_alert=True)
        return

    request_id = int(callback.data.split("_")[2])

    request = await loop.run_in_executor(None, get_request_by_id, request_id)

    if not request:
        await callback.answer("❌ Not found or already processed!", show_alert=True)
        # Try to delete the message anyway
        try:
            await callback.message.delete()
        except:
            pass
        return

    # Check if already processed
    if request['status'] != 'pending':
        await callback.answer(f"⚠️ Already {request['status']}!", show_alert=True)
        # Try to delete the message
        try:
            await callback.message.delete()
        except:
            pass
        return

    user_id = request['user_id']
    username = request['username']
    user_mention = f"@{username}" if username else f"User {user_id}"

    # Update request status
    await loop.run_in_executor(None, update_request_status, request_id, "declined")

    # Delete the request
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM number_requests WHERE id = %s", (request_id,))

    # Notify user in DM
    try:
        await bot.send_message(
            chat_id=user_id,
            text="❌ <b>Your line request was denied by admin.</b>\n\nMessage in the group to find out why.",
            parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"Error notifying user: {e}")

    # Notify group
    try:
        await bot.send_message(
            chat_id=GROUP_CHAT_ID,
            text=f"❌ {user_mention}'s line request has been declined",
            parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"Error notifying group: {e}")

    # Delete the decline message for all admins
    try:
        await callback.message.delete()
    except:
        pass

    await callback.answer("✅ Declined!")


# UNDO DECLINE LINE
@router.callback_query(F.data.startswith("undo_decline_line_"))
async def callback_undo_decline_line(callback: CallbackQuery, bot: Bot):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Admin only!", show_alert=True)
        return

    request_id = int(callback.data.split("_")[3])

    loop = asyncio.get_event_loop()
    request = await loop.run_in_executor(None, get_request_by_id, request_id)

    if not request:
        await callback.answer("Not found!", show_alert=True)
        return

    user_id = request['user_id']
    username = request['username']
    user_mention = f"@{username}" if username else f"User {user_id}"

    # Mark their old line as unused so they can request new one
    def mark_old_line_unused(user_id):
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """UPDATE number_queue 
                       SET is_used = FALSE, 
                           used_by_user_id = NULL, 
                           used_by_username = NULL,
                           status = NULL,
                           call_summary = NULL
                       WHERE used_by_user_id = %s""",
                    (user_id,)
                )

    await loop.run_in_executor(None, mark_old_line_unused, user_id)

    # Update request status to approved
    await loop.run_in_executor(None, update_request_status, request_id, "approved")

    # Notify user in DM to request new line
    try:
        await bot.send_message(
            chat_id=user_id,
            text="✅ <b>Your request has been approved!</b>\n\nClick /start and use the Line button to request your new line.",
            parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"Error notifying user: {e}")

    # Notify group
    try:
        await bot.send_message(
            chat_id=GROUP_CHAT_ID,
            text=f"✅ {user_mention}'s line request has been approved. They can now request their line.",
            parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"Error notifying group: {e}")

    await callback.message.edit_text(
        f"{callback.message.text}\n\n✅ <b>APPROVED</b>",
        parse_mode="HTML"
    )
    await callback.answer("Undone!")


# OTP CALLBACK
@router.callback_query(F.data.startswith("otp_"))
async def callback_otp(callback: CallbackQuery, bot: Bot):
    user_id = int(callback.data.split("_")[1])

    if callback.from_user.id != user_id:
        await callback.answer("Not for you!", show_alert=True)
        return

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, update_record_status, user_id, "OTP")

    username = callback.from_user.username or callback.from_user.first_name
    user_mention = callback.from_user.mention_html()

    # Get user's current line details
    record = await loop.run_in_executor(None, get_user_record, user_id)

    # Get user agent name and reference
    user_info = await loop.run_in_executor(None, get_user_info, user_id)
    agent_name = user_info['agent_name'] if user_info and user_info.get('agent_name') else "Agent"
    reference = user_info['reference'] if user_info else "LG206187"

    # FOUR OPTIONS
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

    # Send line details to ALL admins
    if record:
        admin_message = f"📲 <b>{user_mention} is on call (OTP)</b>\n\n"
        admin_message += f"👤 <b>Agent:</b> {agent_name}\n"
        admin_message += f"🗃️ Reference: <code>{reference}</code>\n\n\n"
        admin_message += f"🎫Line Details:\n\n"

        if record.get('name'):
            admin_message += f"👤 Name: {record['name']}\n"
        admin_message += f"📞 Number: <code>{record['number']}</code>\n"
        if record.get('address'):
            admin_message += f"📍 Address: {record['address']}\n"
        if record.get('email'):
            admin_message += f"📧 Email: {record['email']}\n"

        # Send to master admin
        try:
            await bot.send_message(
                chat_id=ADMIN_ID,
                text=admin_message,
                parse_mode="HTML"
            )
        except Exception as e:
            logging.error(f"Error sending to master admin: {e}")

        # Send to all other admins
        admins = await loop.run_in_executor(None, get_all_admins)
        for admin in admins:
            if admin['user_id'] != ADMIN_ID:
                try:
                    await bot.send_message(
                        chat_id=admin['user_id'],
                        text=admin_message,
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logging.error(f"Error sending to admin {admin['user_id']}: {e}")

    # Notify group
    try:
        await bot.send_message(
            chat_id=GROUP_CHAT_ID,
            text=f"📲 {user_mention} is on call"
        )
    except Exception as e:
        logging.error(f"Error: {e}")

    await callback.answer()


# CALL ENDED CALLBACK - Ask for summary
@router.callback_query(F.data.startswith("call_ended_"))
async def callback_call_ended(callback: CallbackQuery, state: FSMContext, bot: Bot):
    user_id = int(callback.data.split("_")[2])

    if callback.from_user.id != user_id:
        await callback.answer("Not for you!", show_alert=True)
        return

    username = callback.from_user.username or callback.from_user.first_name
    user_mention = callback.from_user.mention_html()

    # Notify group immediately
    try:
        await bot.send_message(
            chat_id=GROUP_CHAT_ID,
            text=f"📵 {user_mention} call ended"
        )
    except Exception as e:
        logging.error(f"Error: {e}")

    # Ask for summary
    await state.set_state(UserStates.waiting_for_call_ended_summary)
    await state.update_data(user_id=user_id, username=username)

    await callback.message.answer(
        "📝 Enter summary for this call:\n\nSend /cancel to abort.",
    )

    await callback.answer()


@router.message(UserStates.waiting_for_call_ended_summary, F.text)
async def receive_call_ended_summary(message: Message, state: FSMContext, bot: Bot):


    data = await state.get_data()
    user_id = data.get('user_id')
    username = data.get('username')
    summary_text = message.text

    loop = asyncio.get_event_loop()
    if message.text.startswith('/cancel'):
        await state.clear()
        await message.answer("❌ Cancelled.")
        await loop.run_in_executor(None, mark_line_completed, user_id)
        return

    await loop.run_in_executor(None, mark_line_completed, user_id)

    # Send summary to group (but don't save)
    try:
        user_mention = message.from_user.mention_html()
        await bot.send_message(
            chat_id=GROUP_CHAT_ID,
            text=f"📝 <b>{user_mention}'s Call Summary:</b>\n\n{summary_text}",
            parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"Error: {e}")

    # Show button to request new line
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Request New Line 🔄", callback_data=f"request_line")]
    ])

    await message.answer(
        "✅ <b>Summary sent to group!</b>\n\n"
        "You can now request a new line if needed.",
        parse_mode="HTML",
        reply_markup=keyboard
    )

    await state.clear()




# FINISHING CALLBACK - Ask for summary
@router.callback_query(F.data.startswith("finishing_"))
async def callback_finishing(callback: CallbackQuery, state: FSMContext, bot: Bot):
    user_id = int(callback.data.split("_")[1])

    if callback.from_user.id != user_id:
        await callback.answer("Not for you!", show_alert=True)
        return

    username = callback.from_user.username or callback.from_user.first_name
    user_mention = callback.from_user.mention_html()

    # Notify group
    try:
        await bot.send_message(
            chat_id=GROUP_CHAT_ID,
            text=f"🫡 {user_mention} is finishing call"
        )
    except Exception as e:
        logging.error(f"Error: {e}")

    # Tell user
    await callback.message.answer(
        "✅ Admin has been told you will be finishing call\n\n"
        "📝 Send summary for this call:",
    )

    # Set state to wait for finishing summary
    await state.set_state(UserStates.waiting_for_finishing_summary)
    await state.update_data(user_id=user_id, username=username)

    await callback.answer()


# RECEIVE FINISHING SUMMARY
@router.message(UserStates.waiting_for_finishing_summary, F.text)
async def receive_finishing_summary(message: Message, state: FSMContext, bot: Bot):
    if message.text.startswith('/cancel'):
        await state.clear()
        await message.answer("❌ Cancelled.")
        return

    data = await state.get_data()
    user_id = data.get('user_id')
    username = data.get('username')
    summary_text = message.text

    loop = asyncio.get_event_loop()

    await loop.run_in_executor(None, mark_line_completed, user_id)

    # Send summary to group (but don't save)
    try:
        user_mention = message.from_user.mention_html()
        await bot.send_message(
            chat_id=GROUP_CHAT_ID,
            text=f"🫡 <b>{user_mention} finishing call</b>\n\n📝 <b>Summary:</b>\n{summary_text}",
            parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"Error: {e}")

    # Show button to request new line
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Request New Line 🔄", callback_data=f"request_line")]
    ])

    await message.answer(
        "✅ <b>Finishing summary sent to group!</b>\n\n"
        "You can now request a new line if needed.",
        parse_mode="HTML",
        reply_markup=keyboard
    )

    await state.clear()



# VIC NEEDS CALLBACK - Ask for summary
@router.callback_query(F.data.startswith("vic_callback_"))
async def callback_vic_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    user_id = int(callback.data.split("_")[2])

    if callback.from_user.id != user_id:
        await callback.answer("Not for you!", show_alert=True)
        return

    username = callback.from_user.username or callback.from_user.first_name
    user_mention = callback.from_user.mention_html()

    # Notify group immediately
    try:
        await bot.send_message(
            chat_id=GROUP_CHAT_ID,
            text=f"☎️ {user_mention} needs a call back"
        )
    except Exception as e:
        logging.error(f"Error: {e}")

    # Ask for summary
    await state.set_state(UserStates.waiting_for_callback_summary)
    await state.update_data(user_id=user_id, username=username)

    await callback.message.answer(
        "📝 Enter summary for callback:\n\nSend /cancel to abort.",
    )

    await callback.answer()


# RECEIVE CALLBACK SUMMARY
@router.message(UserStates.waiting_for_callback_summary, F.text)
async def receive_callback_summary(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    user_id = data.get('user_id')
    username = data.get('username')
    summary_text = message.text

    loop = asyncio.get_event_loop()
    if message.text.startswith('/cancel'):
        await state.clear()
        await message.answer("❌ Cancelled.")
        await loop.run_in_executor(None, mark_line_completed, user_id)
        return

    # Mark line as available (but DON'T save to call_backs table)
    await loop.run_in_executor(None, mark_line_completed, user_id)

    # Send summary to group (but don't save)
    try:
        user_mention = message.from_user.mention_html()
        await bot.send_message(
            chat_id=GROUP_CHAT_ID,
            text=f"☎️ <b>{user_mention} needs callback</b>\n\n📝 <b>Summary:</b>\n{summary_text}",
            parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"Error: {e}")

    # Show button to request new line
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Request New Line 🔄", callback_data=f"request_line")]
    ])

    await message.answer(
        "✅ <b>Callback summary sent to group!</b>\n\n"
        "You can now request a new line if needed.",
        parse_mode="HTML",
        reply_markup=keyboard
    )

    await state.clear()




@router.message(Command("export_callbacks"), F.chat.type == "private", F.from_user.id == ADMIN_ID)
async def cmd_export_callbacks(message: Message, bot: Bot):
    loop = asyncio.get_event_loop()

    def get_all_callbacks():
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """SELECT user_id, username, number, name, address, email, call_summary, created_at 
                       FROM call_backs ORDER BY created_at DESC"""
                )
                return cursor.fetchall()

    callbacks = await loop.run_in_executor(None, get_all_callbacks)

    if not callbacks:
        await message.answer("No callbacks found!")
        return

    # Create CSV
    file_path = "/tmp/callbacks.csv"
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['User ID', 'Username', 'Number', 'Name', 'Address', 'Email', 'Summary', 'Date'])
        for cb in callbacks:
            writer.writerow([
                cb['user_id'],
                cb['username'],
                cb['number'],
                cb['name'],
                cb['address'],
                cb['email'],
                cb['call_summary'],
                cb['created_at']
            ])

    # Send file
    document = FSInputFile(file_path)
    with open(file_path, 'rb') as f:
        await bot.send_document(
            chat_id=message.from_user.id,
            document=document,
            caption="📋 Callback Records"
        )

    await message.answer("✅ Callbacks exported!")


# NEED A PASS CALLBACK
@router.callback_query(F.data.startswith("need_pass_"))
async def callback_need_pass(callback: CallbackQuery, state: FSMContext, bot: Bot):
    user_id = int(callback.data.split("_")[2])

    if callback.from_user.id != user_id:
        await callback.answer("Not for you!", show_alert=True)
        return

    username = callback.from_user.username or callback.from_user.first_name
    user_mention = callback.from_user.mention_html()

    try:
        await bot.send_message(
            chat_id=GROUP_CHAT_ID,
            text=f"⛹️ {user_mention} needs a pass",
            parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"Error: {e}")

    # Ask for summary
    await state.set_state(UserStates.waiting_for_summary)
    await state.update_data(user_id=user_id, need_pass=True)

    await callback.message.answer(
        "📝 <b>Enter your summary before requesting another pass:</b>\n\nSend /cancel to abort.",
        parse_mode="HTML"
    )

    await callback.answer()


@router.callback_query(F.data.startswith("noanswer_"))
async def callback_noanswer(callback: CallbackQuery, bot: Bot):
    user_id = int(callback.data.split("_")[1])

    if callback.from_user.id != user_id:
        await callback.answer("Not for you!", show_alert=True)
        return

    loop = asyncio.get_event_loop()

    # STEP 1: Update status
    await loop.run_in_executor(None, update_record_status, user_id, "No Answer")

    # STEP 2: CRITICAL - Mark line as completed IMMEDIATELY
    await loop.run_in_executor(None, mark_line_completed, user_id)

    username = callback.from_user.username or callback.from_user.first_name
    user_mention = callback.from_user.mention_html()

    # Get user's current line details BEFORE marking complete
    record = await loop.run_in_executor(None, get_user_record, user_id)

    # Get user agent name and reference
    user_info = await loop.run_in_executor(None, get_user_info, user_id)
    agent_name = user_info['agent_name'] if user_info and user_info.get('agent_name') else "Agent"
    reference = user_info['reference'] if user_info else REFERENCE

    # Save no answer record
    if record:
        await loop.run_in_executor(
            None,
            save_no_answer_record,
            user_id,
            username,
            agent_name,
            reference,
            record.get('number'),
            record.get('name'),
            record.get('address'),
            record.get('email')
        )

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Request Another Line 🔄", callback_data=f"request_line")]
    ])

    await callback.message.edit_text(
        f"{callback.message.text}\n\n❌ <b>No Answer</b>\n\nRequest another:",
        parse_mode="HTML",
        reply_markup=keyboard
    )

    await callback.answer()


# SUMMARY CALLBACK
@router.callback_query(F.data.startswith("summary_"))
async def callback_summary(callback: CallbackQuery, state: FSMContext):
    user_id = int(callback.data.split("_")[1])

    if callback.from_user.id != user_id:
        await callback.answer("Not for you!", show_alert=True)
        return

    await state.set_state(UserStates.waiting_for_summary)
    await state.update_data(user_id=user_id)

    await callback.message.answer(
        "📝 <b>Enter your call summary:</b>\n\nSend /cancel to abort.",
        parse_mode="HTML"
    )

    await callback.answer()


# RECEIVE SUMMARY
@router.message(UserStates.waiting_for_summary, F.text)
async def receive_summary(message: Message, state: FSMContext, bot: Bot):


    data = await state.get_data()
    user_id = data.get('user_id')
    need_pass = data.get('need_pass', False)
    summary_text = message.text

    loop = asyncio.get_event_loop()
    if message.text.startswith('/cancel'):
        await state.clear()
        await message.answer("❌ Cancelled.")
        await loop.run_in_executor(None, update_record_summary, user_id, summary_text)
        await loop.run_in_executor(None, mark_line_completed, user_id)
        return

    await loop.run_in_executor(None, update_record_summary, user_id, summary_text)
    await loop.run_in_executor(None, mark_line_completed, user_id)


    username = message.from_user.username or message.from_user.first_name
    user_mention = message.from_user.mention_html()

    # Send summary to group
    try:
        await bot.send_message(
            chat_id=GROUP_CHAT_ID,
            text=f"📋 <b>Summary from {user_mention}</b>\n\n{summary_text}",
            parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"Error: {e}")

    if need_pass:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Request New Line 🔄", callback_data=f"request_line")]
        ])

        await message.answer(
            "✅ <b>Summary submitted!</b>\n\nClick below to request a new line:",
            parse_mode="HTML",
            reply_markup=keyboard
        )
    else:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Request Another Line 🔄", callback_data=f"request_line")]
        ])

        await message.answer("✅ <b>Summary submitted!</b>", parse_mode="HTML", reply_markup=keyboard)

    await state.clear()




# Main
async def main():
    try:
        logging.info("📦 Initializing database...")
        init_database()
        bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        await set_bot_commands(bot)
        dp = Dispatcher(storage=MemoryStorage())
        dp.include_router(router)

        logging.info("🤖 Bot started!")

        await dp.start_polling(bot)

    except Exception as e:
        logging.error(f"❌ Error: {e}")
        raise


if __name__ == '__main__':
    asyncio.run(main())