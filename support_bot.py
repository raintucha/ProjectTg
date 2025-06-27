import telegram  # Добавьте эту строку в импорты
import logging
import os
import re
from datetime import datetime, timedelta
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    CallbackQueryHandler,
)
import psycopg2
from fpdf import FPDF
from io import BytesIO
import asyncio
from threading import Thread
from http.server import BaseHTTPRequestHandler, HTTPServer
import time
from telegram.error import NetworkError, TimedOut

# Явно укажем, что это веб-сервис
WEB_SERVICE = True
PORT = int(os.getenv("PORT", 8080))

# Load configuration
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
DIRECTOR_CHAT_ID = os.getenv("DIRECTOR_CHAT_ID")
NEWS_CHANNEL = os.getenv("NEWS_CHANNEL", "@sunqar_news")
DATABASE_URL = os.getenv("DATABASE_URL")

# Validate environment variables
if not TELEGRAM_TOKEN or not DIRECTOR_CHAT_ID or not DATABASE_URL:
    raise ValueError("Missing required environment variables: TELEGRAM_TOKEN, DIRECTOR_CHAT_ID, or DATABASE_URL")

try:
    DIRECTOR_CHAT_ID = int(DIRECTOR_CHAT_ID)
except ValueError:
    raise ValueError("DIRECTOR_CHAT_ID must be a valid integer")

# Setup logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Role constants
SUPPORT_ROLES = {"user": 1, "agent": 2, "admin": 3}
USER_TYPES = {"resident": "resident", "potential_buyer": "potential_buyer"}
def init_db():
    """Initialize database tables if they don't exist."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Создание таблицы users
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    full_name TEXT NOT NULL,
                    role INTEGER NOT NULL,
                    registration_date TIMESTAMP NOT NULL
                )
            """)
            
            # Создание таблицы residents
            cur.execute("""
                CREATE TABLE IF NOT EXISTS residents (
                    resident_id SERIAL PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    full_name TEXT NOT NULL,
                    address TEXT NOT NULL,
                    phone TEXT NOT NULL,
                    registration_date TIMESTAMP NOT NULL
                )
            """)
            
            # Остальные таблицы...
            
            conn.commit()
            logger.info("Database tables initialized")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def get_db_connection():
    """Establish database connection using DATABASE_URL."""
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        logger.info("Successfully connected to database")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
        raise

async def get_user_role(user_id: int) -> int:
    """Retrieve user role from database."""
    if user_id == DIRECTOR_CHAT_ID:
        return SUPPORT_ROLES["admin"]
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT role FROM users WHERE user_id = %s", (user_id,))
            result = cur.fetchone()
            return result[0] if result else SUPPORT_ROLES["user"]
    except psycopg2.Error as e:
        logger.error(f"Error retrieving user role: {e}")
        return SUPPORT_ROLES["user"]
    finally:
        if conn:
            conn.close()

async def is_admin(user_id: int) -> bool:
    """Check if user is an admin."""
    return await get_user_role(user_id) == SUPPORT_ROLES["admin"]

async def is_agent(user_id: int) -> bool:
    """Check if user is an agent or admin."""
    role = await get_user_role(user_id)
    return role in [SUPPORT_ROLES["agent"], SUPPORT_ROLES["admin"]]

async def delete_previous_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Delete previous bot messages if they exist."""
    if "last_message_id" not in context.user_data:
        return

    try:
        await context.bot.delete_message(
            chat_id=update.effective_chat.id,
            message_id=context.user_data["last_message_id"],
        )
    except telegram.error.BadRequest as e:
        if "message to delete not found" in str(e):
            logger.warning(f"Message {context.user_data['last_message_id']} already deleted")
        else:
            logger.error(f"Failed to delete message: {e}")
    finally:
        context.user_data.pop("last_message_id", None)

async def send_message_with_keyboard(update, context, text, keyboard):
    """Send a message with a keyboard and store its ID, deleting previous message."""
    await delete_previous_messages(update, context)
    try:
        message = await update.effective_chat.send_message(
            text, reply_markup=keyboard
        )
        context.user_data["last_message_id"] = message.message_id
        return message
    except Exception as e:
        logger.error(f"Error sending message: {e}")
        raise

async def send_and_remember(
    update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None
):
    """Send message and store its ID, deleting previous message with retry logic."""
    logger.info(f"Sending message to user {update.effective_user.id}: {text[:50]}...")
    await delete_previous_messages(update, context)
    retries = 3
    for attempt in range(retries):
        try:
            message = await update.effective_chat.send_message(
                text, reply_markup=reply_markup
            )
            context.user_data["last_message_id"] = message.message_id
            logger.info(f"Message sent, ID {message.message_id} stored for user {update.effective_user.id}")
            return message
        except (NetworkError, TimedOut) as e:
            logger.warning(f"Network error on attempt {attempt + 1}: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s, 4s
                continue
            logger.error(f"Failed to send message after {retries} attempts: {e}")
            await update.effective_chat.send_message(
                "❌ Ошибка сети. Попробуйте снова позже.",
                reply_markup=main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id))
            )
            raise
        except Exception as e:
            logger.error(f"Error sending message to user {update.effective_user.id}: {e}")
            await update.effective_chat.send_message(
                "❌ Ошибка при отправке сообщения. Попробуйте снова.",
                reply_markup=main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id))
            )
            raise

async def safe_db_connection(retries=3, delay=2):
    """Try to establish DB connection with retries."""
    for attempt in range(retries):
        try:
            conn = get_db_connection()
            return conn
        except Exception as e:
            logger.warning(f"DB connection attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(delay)
            else:
                raise

async def send_text_with_keyboard(update, context, text, keyboard=None):
    """Helper to send message with keyboard, deleting previous message if any."""
    await delete_previous_messages(update, context)
    try:
        message = await update.effective_chat.send_message(
            text, reply_markup=keyboard
        )
        context.user_data["last_message_id"] = message.message_id
        return message
    except Exception as e:
        logger.error(f"Error sending message: {e}")
        raise

async def safe_send_message(update, context, text, keyboard=None):
    """Wrapper to safely send message with keyboard and handle errors."""
    try:
        return await send_text_with_keyboard(update, context, text, keyboard)
    except Exception as e:
        logger.error(f"Failed to send message: {e}")

async def clear_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /clear command to fully reset chat history as fast as possible."""
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    
    try:
        context.user_data.clear()
        context.chat_data.clear()
        
        current_message_id = update.message.message_id
        message_ids = list(range(max(1, current_message_id - 1000), current_message_id + 1))
        
        async def delete_single_message(msg_id):
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
            except Exception:
                pass
        
        batch_size = 50
        for i in range(0, len(message_ids), batch_size):
            batch = message_ids[i:i + batch_size]
            await asyncio.gather(*[delete_single_message(msg_id) for msg_id in batch])
        
        await update.message.reply_text(
            "🧹 Чат полностью очищен! Нажмите /start, чтобы начать заново."
        )
    except Exception as e:
        logger.error(f"Error clearing chat: {e}")
        await update.message.reply_text(
            "❌ Не удалось полностью очистить чат. Попробуйте снова или используйте /start."
        )

async def shutdown_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Initiate bot shutdown with confirmation."""
    if not await is_admin(update.effective_user.id):
        await update.callback_query.answer("❌ Доступ запрещен", show_alert=True)
        return
    keyboard = [
        [InlineKeyboardButton("✅ Да, остановить", callback_data="confirm_shutdown")],
        [InlineKeyboardButton("❌ Нет, отмена", callback_data="cancel_shutdown")],
    ]
    await safe_send_message(
        update,
        context,
        "⚠️ Вы уверены, что хотите остановить бота?",
        InlineKeyboardMarkup(keyboard),
    )

async def confirm_shutdown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clean shutdown of the bot."""
    if not await is_admin(update.effective_user.id):
        await update.callback_query.answer("❌ Доступ запрещен", show_alert=True)
        return
    await safe_send_message(update, context, "🛑 Бот останавливается...")
    import sys
    sys.exit(0)

async def process_report_period(
    update: Update, context: ContextTypes.DEFAULT_TYPE, period_type: str
):
    """Process selected report period."""
    end_date = datetime.now()
    if period_type == "7":
        start_date = end_date - timedelta(days=7)
    elif period_type == "30":
        start_date = end_date - timedelta(days=30)
    elif period_type == "month":
        start_date = end_date.replace(day=1)
    else:
        await safe_send_message(
            update,
            context,
            "❌ Неверный период отчета.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
        return
    await generate_and_send_report(update, context, start_date, end_date)

async def process_user_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process user phone and save resident data with comprehensive validation."""
    phone = update.message.text.strip()
    logger.info(f"Received phone input for user {update.effective_user.id}: {phone}")
    phone_pattern = re.compile(r"^\+?\d{7,15}$")
    if not phone_pattern.match(phone):
        logger.warning(f"Invalid phone format: {phone}")
        await send_and_remember(
            update,
            context,
            "❌ Неверный формат телефона. Пожалуйста, введите корректный номер (например, +71234567890):",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]),
        )
        return

    phone = re.sub(r"[^\d+]", "", phone)
    
    conn = None
    try:
        required_fields = {
            'user_name': str,
            'user_address': str,
            'problem_text': str,
            'is_urgent': bool
        }
        
        missing_fields = [field for field in required_fields if field not in context.user_data]
        if missing_fields:
            logger.error(f"Missing required fields for user {update.effective_user.id}: {missing_fields}")
            await send_and_remember(
                update,
                context,
                f"❌ Ошибка: отсутствуют данные ({', '.join(missing_fields)}). Начните заново.",
                main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
            )
            return

        type_errors = []
        for field, field_type in required_fields.items():
            if not isinstance(context.user_data[field], field_type):
                type_errors.append(f"{field} должен быть {field_type.__name__}")
        
        if type_errors:
            logger.error(f"Type errors for user {update.effective_user.id}: {type_errors}")
            await send_and_remember(
                update,
                context,
                "❌ Ошибка в формате данных. Пожалуйста, начните процесс заново.",
                main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
            )
            return

        conn = get_db_connection()
        logger.info(f"Connected to database for user {update.effective_user.id}")
        with conn.cursor() as cur:
            logger.info(f"Checking if resident exists for chat_id {update.effective_user.id}")
            cur.execute(
                "SELECT resident_id FROM residents WHERE chat_id = %s",
                (update.effective_user.id,)
            )
            if cur.fetchone():
                logger.info(f"Resident already exists for chat_id {update.effective_user.id}")
                await send_and_remember(
                    update,
                    context,
                    "ℹ️ Вы уже зарегистрированы. Создаем новую заявку.",
                    main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
                )

            cur.execute(
                """INSERT INTO residents (chat_id, full_name, address, phone, registration_date)
                VALUES (%s, %s, %s, %s, %s) RETURNING resident_id""",
                (
                    update.effective_user.id,
                    context.user_data["user_name"],
                    context.user_data["user_address"],
                    phone,
                    datetime.now(),
                ),
            )
            resident_id = cur.fetchone()[0]
            logger.info(f"Inserted resident ID {resident_id}")

            cur.execute(
                """INSERT INTO issues (resident_id, description, category, status, created_at)
                VALUES (%s, %s, %s, %s, %s) RETURNING issue_id""",
                (
                    resident_id,
                    context.user_data["problem_text"],
                    "urgent" if context.user_data["is_urgent"] else "normal",
                    "new",
                    datetime.now(),
                ),
            )
            issue_id = cur.fetchone()[0]
            logger.info(f"Inserted issue ID {issue_id}")

            cur.execute(
                """INSERT INTO issue_logs (issue_id, action, user_id, action_time)
                VALUES (%s, 'create', %s, NOW())""",
                (issue_id, update.effective_user.id)
            )
            logger.info(f"Logged issue creation for issue ID {issue_id}")
            
            conn.commit()

            if context.user_data["is_urgent"]:
                await send_urgent_alert(update, context, issue_id)

            await send_and_remember(
                update,
                context,
                "✅ Регистрация и заявка приняты!\n\n"
                f"{'🚨 Срочное обращение! Директор уведомлен.' if context.user_data['is_urgent'] else '⏳ Ожидайте ответа в течение 24 часов.'}\n"
                f"Номер заявки: #{issue_id}",
                main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
            )
            
            context.user_data.clear()
            logger.info(f"Cleared user_data for user {update.effective_user.id}")

    except psycopg2.Error as e:
        logger.error(f"Database error during registration for user {update.effective_user.id}: {e}")
        await send_and_remember(
            update,
            context,
            f"❌ Ошибка базы данных: {e}",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
        if conn:
            conn.rollback()
    except Exception as e:
        logger.error(f"Unexpected error during registration for user {update.effective_user.id}: {e}", exc_info=True)
        await send_and_remember(
            update,
            context,
            f"❌ Непредвиденная ошибка: {e}",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
        if conn:
            conn.rollback()
    finally:
        if conn:
            logger.info("Closing database connection")
            conn.close()
            
async def save_agent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Save new agent to database."""
    if (
        "new_agent_id" not in context.user_data
        or "awaiting_agent_name" not in context.user_data
    ):
        await safe_send_message(
            update,
            context,
            "❌ Ошибка: данные агента не найдены.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
        return
    agent_name = update.message.text
    agent_id = context.user_data["new_agent_id"]
    conn = None
    try:
        conn = await safe_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM users WHERE user_id = %s", (agent_id,))
            if cur.fetchone():
                await safe_send_message(
                    update,
                    context,
                    "❌ Пользователь с таким ID уже существует.",
                    main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
                )
                return
            cur.execute(
                """
                INSERT INTO users (user_id, full_name, role, registration_date)
                VALUES (%s, %s, %s, %s)
                """,
                (agent_id, agent_name, SUPPORT_ROLES["agent"], datetime.now()),
            )
            conn.commit()
        await safe_send_message(
            update,
            context,
            f"✅ Новый агент {agent_name} (ID: {agent_id}) успешно добавлен!",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
        context.user_data.pop("new_agent_id", None)
        context.user_data.pop("awaiting_agent_name", None)
    except psycopg2.Error as e:
        logger.error(f"Error adding agent: {e}")
        await safe_send_message(
            update,
            context,
            "❌ Ошибка при добавлении агента.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
    finally:
        if conn:
            conn.close()

def main_menu_keyboard(user_id, role, is_in_main_menu=False, user_type=None):
    """Generate main menu keyboard based on user role and type."""
    keyboard = []
    if user_type == USER_TYPES["potential_buyer"]:
        keyboard.append([InlineKeyboardButton("🏠 Информация о ЖК", callback_data="complex_info")])
        keyboard.append([InlineKeyboardButton("💰 Цена за м²", callback_data="pricing_info")])
        keyboard.append([InlineKeyboardButton("👥 Отдел продаж", callback_data="sales_team")])
    elif user_type == USER_TYPES["resident"] or role == SUPPORT_ROLES["user"]:
        keyboard.append([InlineKeyboardButton("➕ Новая заявка", callback_data="new_request")])
        keyboard.append([InlineKeyboardButton("📋 Мои заявки", callback_data="my_requests")])
        keyboard.append([InlineKeyboardButton("ℹ️ Помощь", callback_data="help")])
    elif role == SUPPORT_ROLES["agent"]:
        keyboard.append([InlineKeyboardButton("📬 Активные заявки", callback_data="active_requests")])
        keyboard.append([InlineKeyboardButton("🚨 Срочные заявки", callback_data="urgent_requests")])
        keyboard.append([InlineKeyboardButton("📖 Завершенные заявки", callback_data="completed_requests")])
    elif role == SUPPORT_ROLES["admin"] or user_id == DIRECTOR_CHAT_ID:
        keyboard.append([InlineKeyboardButton("📊 Отчеты", callback_data="reports_menu")])
        keyboard.append([InlineKeyboardButton("👥 Управление персоналом", callback_data="manage_agents")])
        keyboard.append([InlineKeyboardButton("🏠 Добавить резидента", callback_data="add_resident")])
        keyboard.append([InlineKeyboardButton("🗑 Удалить резидента", callback_data="delete_resident")])
        keyboard.append([InlineKeyboardButton("📬 Активные заявки", callback_data="active_requests")])
        keyboard.append([InlineKeyboardButton("🚨 Срочные заявки", callback_data="urgent_requests")])
        keyboard.append([InlineKeyboardButton("📖 Завершенные заявки", callback_data="completed_requests")])
        keyboard.append([InlineKeyboardButton("🛑 Завершить работу бота", callback_data="shutdown_bot")])

    btn = InlineKeyboardButton("🔙 Главное меню", callback_data="start")
    if is_in_main_menu:
        btn = InlineKeyboardButton("📍 Вы в главном меню", callback_data="do_nothing")

    keyboard.append([btn])
    return InlineKeyboardMarkup(keyboard)

from telegram.ext import CommandHandler

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command and register user if they are a resident."""
    chat_id = update.effective_user.id
    full_name = update.effective_user.full_name or "Unknown"
    username = update.effective_user.username

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Check if the user is a resident
            cur.execute("SELECT resident_id FROM residents WHERE chat_id = %s", (chat_id,))
            resident = cur.fetchone()
            if resident:
                resident_id = resident[0]
                # Insert or update users table
                cur.execute(
                    """
                    INSERT INTO users (user_id, username, full_name, role, registration_date)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username, full_name = EXCLUDED.full_name, role = EXCLUDED.role, registration_date = EXCLUDED.registration_date
                    """,
                    (chat_id, username, full_name, SUPPORT_ROLES["user"], datetime.now()),
                )
                conn.commit()
                await update.message.reply_text(
                    "🏠 Добро пожаловать обратно, резидент ЖК Сункар! Используйте /new_issue для подачи заявки.",
                    reply_markup=main_menu_keyboard(chat_id, await get_user_role(chat_id))
                )
            else:
                await update.message.reply_text(
                    "❌ Вы не зарегистрированы как резидент. Обратитесь к администратору для добавления.",
                    reply_markup=main_menu_keyboard(chat_id, await get_user_role(chat_id))
                )
    except psycopg2.Error as e:
        logger.error(f"Database error in /start: {e.pgerror if hasattr(e, 'pgerror') else str(e)}")
        await update.message.reply_text("❌ Ошибка базы данных. Попробуйте позже.")
        conn.rollback()
    finally:
        conn.close()

async def select_user_type(update: Update, context: ContextTypes.DEFAULT_TYPE, user_type: str):
    """Set the user type and show the main menu."""
    user_id = update.effective_user.id
    context.user_data["user_type"] = user_type
    role = await get_user_role(user_id)
    await send_and_remember(
        update,
        context,
        f"🏠 Вы вошли как {'житель' if user_type == USER_TYPES['resident'] else 'потенциальный покупатель'}.\n\nВыберите действие:",
        main_menu_keyboard(user_id, role, is_in_main_menu=True, user_type=user_type),
    )

async def process_new_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Initiate new request process."""
    chat_id = update.effective_user.id
    full_name = update.effective_user.full_name or "Unknown"
    username = update.effective_user.username
    logger.info(f"User {chat_id} started new request process")

    # Check and register user in users table if missing
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM users WHERE user_id = %s", (chat_id,))
            if not cur.fetchone():
                cur.execute(
                    """
                    INSERT INTO users (user_id, username, full_name, role, registration_date)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username, full_name = EXCLUDED.full_name, role = EXCLUDED.role, registration_date = EXCLUDED.registration_date
                    """,
                    (chat_id, username, full_name, SUPPORT_ROLES["user"], datetime.now()),
                )
                conn.commit()
                logger.info(f"Auto-registered user {chat_id} in users table")
    except psycopg2.Error as e:
        logger.error(f"Database error in process_new_request: {e}")
        conn.rollback()
    finally:
        conn.close()

    await send_and_remember(
        update,
        context,
        "✍️ Опишите вашу проблему:",
        InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]),
    )
    logger.info(f"Prompted user {chat_id} to describe problem")
    context.user_data["awaiting_problem"] = True
    logger.info(f"Set awaiting_problem for user {chat_id}")

async def show_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display help information."""
    logger.info(f"Showing help for user {update.effective_user.id}")
    try:
        await send_and_remember(
            update,
            context,
            f"ℹ️ Справка:\n\n• Для срочных проблем используйте слова: 'потоп', 'пожар', 'авария'\n"
            f"• Новости ЖК: {NEWS_CHANNEL}\n• Техподдержка: @ShiroOni99",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
        logger.info(f"Help message sent to user {update.effective_user.id}")
    except Exception as e:
        logger.error(f"Error in show_help for user {update.effective_user.id}: {e}", exc_info=True)
        await send_and_remember(
            update,
            context,
            "❌ Ошибка при отображении справки. Попробуйте позже.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )

async def show_user_requests(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user's recent requests."""
    logger.info(f"Showing requests for user {update.effective_user.id}")
    conn = None
    try:
        conn = get_db_connection()
        logger.info("Database connection established")
        with conn.cursor() as cur:
            # Check if table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'residents'
                )
            """)
            if not cur.fetchone()[0]:
                logger.error("Table 'residents' does not exist")
                await send_and_remember(
                    update,
                    context,
                    "❌ Ошибка: таблица residents не найдена.",
                    main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
                )
                return
            # Check issues table
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'issues'
                )
            """)
            if not cur.fetchone()[0]:
                logger.error("Table 'issues' does not exist")
                await send_and_remember(
                    update,
                    context,
                    "❌ Ошибка: таблица issues не найдена.",
                    main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
                )
                return

            cur.execute(
                """
                SELECT i.issue_id, i.description, i.category, i.status, i.created_at 
                FROM issues i
                JOIN residents r ON i.resident_id = r.resident_id
                WHERE r.chat_id = %s
                ORDER BY i.created_at DESC
                LIMIT 5
                """,
                (update.effective_user.id,),
            )
            requests = cur.fetchall()
            logger.info(f"Found {len(requests)} requests for user {update.effective_user.id}")

        if not requests:
            await send_and_remember(
                update,
                context,
                "📭 У вас пока нет заявок.",
                main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
            )
            return

        text = "📋 Ваши последние заявки:\n\n"
        for req in requests:
            text += (
                f"🆔 Номер: #{req[0]}\n"
                f"📅 Дата: {req[4].strftime('%d.%m.%Y %H:%M')}\n"
                f"🚨 Тип: {'Срочная' if req[2] == 'urgent' else 'Обычная'}\n"
                f"📝 Описание: {req[1][:100]}{'...' if len(req[1]) > 100 else ''}\n"
                f"🟢 Статус: {req[3]}\n\n"
            )

        await send_and_remember(
            update,
            context,
            text,
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
    except psycopg2.Error as e:
        logger.error(f"Error retrieving user requests for {update.effective_user.id}: {e}")
        await send_and_remember(
            update,
            context,
            f"❌ Ошибка базы данных: {e}",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
    finally:
        if conn:
            logger.info("Closing database connection")
            conn.close()

async def process_problem_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process problem description."""
    problem_text = update.message.text
    logger.info(f"User {update.effective_user.id} entered problem: {problem_text}")
    context.user_data["problem_text"] = problem_text
    urgent_keywords = ["потоп", "затоп", "пожар", "авария", "срочно", "опасно"]
    is_urgent = any(keyword in problem_text.lower() for keyword in urgent_keywords)
    context.user_data["is_urgent"] = is_urgent
    logger.info(f"Urgency detected: {is_urgent}")
    
    # Check and register user in users table if missing
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM users WHERE user_id = %s", (update.effective_user.id,))
            if not cur.fetchone():
                username = update.effective_user.username
                full_name = update.effective_user.full_name or "Unknown"
                cur.execute(
                    """
                    INSERT INTO users (user_id, username, full_name, role, registration_date)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username, full_name = EXCLUDED.full_name, role = EXCLUDED.role, registration_date = EXCLUDED.registration_date
                    """,
                    (update.effective_user.id, username, full_name, SUPPORT_ROLES["user"], datetime.now()),
                )
                conn.commit()
                logger.info(f"Auto-registered user {update.effective_user.id} in users table")
    except psycopg2.Error as e:
        logger.error(f"Database error in process_problem_report: {e}")
        conn.rollback()
    finally:
        conn.close()

    # Proceed with resident check
    conn = None
    try:
        conn = get_db_connection()
        logger.info(f"Connected to database for user {update.effective_user.id}")
        with conn.cursor() as cur:
            logger.info(f"Checking resident for chat_id {update.effective_user.id}")
            cur.execute(
                "SELECT resident_id FROM residents WHERE chat_id = %s",
                (update.effective_user.id,)
            )
            resident = cur.fetchone()
            logger.info(f"Resident found: {resident is not None}")
    except psycopg2.Error as e:
        logger.error(f"Database error in resident check: {e}")
        await send_and_remember(
            update,
            context,
            "❌ Ошибка базы данных при проверке резидента. Попробуйте позже.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id))
        )
        return
    finally:
        if conn:
            logger.info("Closing database connection")
            conn.close()

async def save_request_to_db(update: Update, context: ContextTypes.DEFAULT_TYPE, resident_id: int):
    logger.info(f"Attempting to save request. Resident ID: {resident_id}")
    logger.info(f"Context data: {context.user_data}")
    user_id = update.effective_user.id
    
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Ensure user exists in users table
            cur.execute("SELECT 1 FROM users WHERE user_id = %s", (user_id,))
            if not cur.fetchone():
                username = update.effective_user.username
                full_name = update.effective_user.full_name or "Unknown"
                cur.execute(
                    """
                    INSERT INTO users (user_id, username, full_name, role, registration_date)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username, full_name = EXCLUDED.full_name, role = EXCLUDED.role, registration_date = EXCLUDED.registration_date
                    """,
                    (user_id, username, full_name, SUPPORT_ROLES["user"], datetime.now()),
                )
                conn.commit()
                logger.info(f"Auto-registered user {user_id} in users table")

            cur.execute("SELECT 1 FROM residents WHERE resident_id = %s", (resident_id,))
            if not cur.fetchone():
                logger.error(f"Resident {resident_id} not found in database")
                raise ValueError(f"Resident {resident_id} not found")
            
            required_fields = ['problem_text', 'is_urgent']
            for field in required_fields:
                if field not in context.user_data:
                    logger.error(f"Missing required field: {field}")
                    raise ValueError(f"Missing {field}")
            
            cur.execute(
                """INSERT INTO issues (resident_id, description, category, status, created_at)
                VALUES (%s, %s, %s, %s, %s) RETURNING issue_id""",
                (
                    resident_id,
                    context.user_data["problem_text"],
                    "urgent" if context.user_data["is_urgent"] else "normal",
                    "new",
                    datetime.now()
                )
            )
            issue_id = cur.fetchone()[0]

            # Log the issue creation
            cur.execute(
                """
                INSERT INTO issue_logs (issue_id, action, user_id, action_time)
                VALUES (%s, 'create', %s, NOW())
                """,
                (issue_id, user_id)
            )
            conn.commit()
            
            logger.info(f"Successfully saved issue #{issue_id} with log")
            return issue_id
            
    except psycopg2.IntegrityError as e:
        logger.error(f"Integrity error: {e}")
        raise ValueError("Database integrity error") from e
    except Exception as e:
        logger.error(f"Error saving request: {e}")
        raise
    finally:
        if conn:
            conn.close()

async def send_urgent_alert(
    update: Update, context: ContextTypes.DEFAULT_TYPE, issue_id: int
):
    """Send urgent alert to director."""
    try:
        user = update.effective_user
        await context.bot.send_message(
            chat_id=DIRECTOR_CHAT_ID,
            text=(
                f"🚨 СРОЧНОЕ ОБРАЩЕНИЕ #{issue_id} 🚨\n\n"
                f"От: {user.full_name} (@{user.username or 'нет'})\n"
                f"ID: {user.id}\n"
                f"Проблема: {context.user_data['problem_text']}\n"
                f"Время: {datetime.now().strftime('%H:%M %d.%m.%Y')}"
            ),
        )
    except Exception as e:
        logger.error(f"Error sending urgent alert: {e}")

async def process_user_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process user full name."""
    context.user_data["user_name"] = update.message.text
    context.user_data.pop("awaiting_name", None)
    context.user_data["awaiting_address"] = True
    await send_and_remember(
        update,
        context,
        "🏠 Введите ваш адрес (например: Корпус 1, кв. 25):",
        InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]),
    )

async def process_user_address(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process user address."""
    context.user_data["user_address"] = update.message.text
    context.user_data.pop("awaiting_address", None)
    context.user_data["awaiting_phone"] = True
    await send_and_remember(
        update,
        context,
        "📱 Введите ваш контактный телефон:",
        InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]),
    )

async def show_active_requests(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show active requests for agents."""
    if not await is_agent(update.effective_user.id):
        await update.callback_query.answer("❌ Доступ запрещен", show_alert=True)
        return
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT i.issue_id, r.full_name, i.description, i.created_at, i.category
                FROM issues i
                JOIN residents r ON i.resident_id = r.resident_id
                WHERE i.status = 'new'
                ORDER BY i.created_at DESC
                LIMIT 20
                """
            )
            requests = cur.fetchall()

        if not requests:
            await send_and_remember(
                update,
                context,
                "📭 Нет активных заявок.",
                main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
            )
            return

        keyboard = [
            [
                InlineKeyboardButton(
                    f"{'🚨' if req[4] == 'urgent' else '📋'} #{req[0]} от {req[1]}",
                    callback_data=f"request_detail_{req[0]}",
                )
            ]
            for req in requests
        ]
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")])

        await send_and_remember(
            update,
            context,
            "📋 Активные заявки:",
            InlineKeyboardMarkup(keyboard),
        )
    except psycopg2.Error as e:
        logger.error(f"Error retrieving active requests: {e}")
        await send_and_remember(
            update,
            context,
            "❌ Ошибка при получении данных.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
    finally:
        if conn:
            conn.close()

async def show_request_detail(
    update: Update, context: ContextTypes.DEFAULT_TYPE, issue_id: int
):
    """Show request details with completion option."""
    if not await is_agent(update.effective_user.id):
        await update.callback_query.answer("❌ Доступ запрещен", show_alert=True)
        return
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT i.issue_id, r.full_name, i.description, i.created_at, i.category, r.chat_id
                FROM issues i
                JOIN residents r ON i.resident_id = r.resident_id
                WHERE i.issue_id = %s
                """,
                (issue_id,),
            )
            request = cur.fetchone()

        if not request:
            await update.callback_query.answer("Заявка не найдена", show_alert=True)
            return

        text = (
            f"🆔 Номер: #{request[0]}\n"
            f"👤 От: {request[1]}\n"
            f"📅 Дата: {request[3].strftime('%d.%m.%Y %H:%M')}\n"
            f"🚨 Тип: {'Срочная' if request[4] == 'urgent' else 'Обычная'}\n"
            f"📝 Описание: {request[2]}"
        )
        keyboard = [
            [
                InlineKeyboardButton(
                    "✅ Завершить заявку", callback_data=f"complete_request_{issue_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    "📨 Написать пользователю", callback_data=f"message_user_{request[5]}"
                )
            ],
            [InlineKeyboardButton("🔙 Назад к списку", callback_data="active_requests")],
        ]

        await send_and_remember(
            update,
            context,
            text,
            InlineKeyboardMarkup(keyboard),
        )
    except psycopg2.Error as e:
        logger.error(f"Error retrieving request details: {e}")
        await send_and_remember(
            update,
            context,
            "❌ Ошибка при получении данных.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
    finally:
        if conn:
            conn.close()

async def complete_request(
    update: Update, context: ContextTypes.DEFAULT_TYPE, issue_id: int
):
    """Initiate request completion process."""
    if not await is_agent(update.effective_user.id):
        await update.callback_query.answer("❌ Доступ запрещен", show_alert=True)
        return
    await send_and_remember(
        update,
        context,
        "✍️ Опишите решение по заявке:",
        InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Отмена", callback_data=f"request_detail_{issue_id}")]]
        ),
    )
    context.user_data["awaiting_solution"] = True
    context.user_data["current_issue_id"] = issue_id

async def save_solution(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Save solution and complete request."""
    if "current_issue_id" not in context.user_data:
        logger.error("No current_issue_id in context.user_data")
        await send_and_remember(
            update,
            context,
            "❌ Ошибка: не найдена текущая заявка.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
        return

    solution = update.message.text
    issue_id = context.user_data["current_issue_id"]
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT i.resident_id, r.chat_id 
                FROM issues i
                JOIN residents r ON i.resident_id = r.resident_id
                WHERE i.issue_id = %s
                """,
                (issue_id,),
            )
            issue_data = cur.fetchone()
            if not issue_data:
                logger.error(f"Issue #{issue_id} not found in database")
                await send_and_remember(
                    update,
                    context,
                    f"❌ Заявка #{issue_id} не найдена.",
                    main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
                )
                return
            resident_id, resident_chat_id = issue_data

            cur.execute(
                """
                UPDATE issues 
                SET status = 'completed', 
                    solution = %s,
                    completed_at = NOW(),
                    closed_by = %s
                WHERE issue_id = %s
                """,
                (solution, update.effective_user.id, issue_id),
            )
            cur.execute(
                """
                INSERT INTO issue_logs (issue_id, action, user_id, action_time)
                VALUES (%s, 'complete', %s, NOW())
                """,
                (issue_id, update.effective_user.id),
            )
            conn.commit()

        try:
            await context.bot.send_message(
                chat_id=resident_chat_id,
                text=f"✅ Ваша заявка #{issue_id} завершена!\n\nРешение: {solution}",
            )
        except Exception as e:
            logger.error(f"Failed to notify user {resident_chat_id}: {e}")

        await send_and_remember(
            update,
            context,
            f"✅ Заявка #{issue_id} успешно завершена!\nПользователь уведомлен.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
    except psycopg2.Error as e:
        logger.error(f"Database error completing issue #{issue_id}: {e}")
        await send_and_remember(
            update,
            context,
            f"❌ Ошибка базы данных при завершении заявки: {e}",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
        if conn:
            conn.rollback()
    finally:
        context.user_data.pop("awaiting_solution", None)
        context.user_data.pop("current_issue_id", None)
        if conn:
            conn.close()

async def show_urgent_requests(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show urgent requests for agents."""
    if not await is_agent(update.effective_user.id):
        await update.callback_query.answer("❌ Доступ запрещен", show_alert=True)
        return
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT i.issue_id, r.full_name, i.description, i.created_at
                FROM issues i
                JOIN residents r ON i.resident_id = r.resident_id
                WHERE i.status = 'new' AND i.category = 'urgent'
                ORDER BY i.created_at DESC
                LIMIT 20
                """
            )
            requests = cur.fetchall()

        if not requests:
            await send_and_remember(
                update,
                context,
                "📭 Нет срочных заявок.",
                main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
            )
            return

        keyboard = [
            [
                InlineKeyboardButton(
                    f"🚨 #{req[0]} от {req[1]}", callback_data=f"request_detail_{req[0]}"
                )
            ]
            for req in requests
        ]
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")])

        await send_and_remember(
            update,
            context,
            "🚨 Срочные заявки:",
            InlineKeyboardMarkup(keyboard),
        )
    except psycopg2.Error as e:
        logger.error(f"Error retrieving urgent requests: {e}")
        await send_and_remember(
            update,
            context,
            "❌ Ошибка при получении данных.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
    finally:
        if conn:
            conn.close()

async def completed_requests(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show completed requests."""
    if not await is_agent(update.effective_user.id):
        await update.callback_query.answer("❌ Доступ запрещен", show_alert=True)
        return
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT i.issue_id, r.full_name, r.address, i.description, i.category, 
                       i.created_at, i.completed_at, COALESCE(u.full_name, 'Не указан') as closed_by
                FROM issues i
                JOIN residents r ON i.resident_id = r.resident_id
                LEFT JOIN users u ON i.closed_by = u.user_id
                WHERE i.status = 'completed'
                ORDER BY i.completed_at DESC
                LIMIT 20
                """
            )
            issues = cur.fetchall()

        if not issues:
            await send_and_remember(
                update,
                context,
                "📖 Нет завершенных заявок",
                main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
            )
            return

        text = "📖 Завершенные заявки:\n\n"
        for issue in issues:
            text += (
                f"🆔 Номер: #{issue[0]}\n"
                f"👤 От: {issue[1]}\n"
                f"🏠 Адрес: {issue[2]}\n"
                f"📝 Описание: {issue[3][:100]}{'...' if len(issue[3]) > 100 else ''}\n"
                f"📅 Создано: {issue[5].strftime('%d.%m.%Y %H:%M')}\n"
                f"✅ Завершено: {issue[6].strftime('%d.%m.%Y %H:%M') if issue[6] else 'Не указано'}\n"
                f"👷 Закрыл: {issue[7]}\n"
                f"{'🚨 Срочная' if issue[4] == 'urgent' else '📋 Обычная'}\n\n"
            )

        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]

        await send_and_remember(
            update,
            context,
            text,
            InlineKeyboardMarkup(keyboard),
        )
    except psycopg2.Error as e:
        logger.error(f"Database error in completed_requests: {e}")
        await send_and_remember(
            update,
            context,
            f"❌ Ошибка базы данных: {e}",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
    finally:
        if conn:
            conn.close()

            

import os
import re
import logging
from io import BytesIO
from fpdf import FPDF
from datetime import datetime
import psycopg2

logger = logging.getLogger(__name__)

def generate_pdf_report(start_date, end_date):
    """Generate properly aligned PDF report"""
    pdf = FPDF()
    conn = None
    try:
        # Подключение шрифта
        font_path = "DejaVuSans.ttf"
        if not os.path.exists(font_path):
            logger.error(f"Font file {font_path} not found.")
            raise Exception(f"Font file {font_path} not found.")

        pdf.add_font("DejaVuSans", "", font_path, uni=True)
        pdf.add_font("DejaVuSans", "B", font_path, uni=True)
        pdf.set_font("DejaVuSans", "", 10)

        # Подключение к базе
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT r.full_name, r.address, i.description, 
                       i.category, i.status, COALESCE(u.full_name, 'Не указан') as closed_by
                FROM issues i
                JOIN residents r ON i.resident_id = r.resident_id
                LEFT JOIN users u ON i.closed_by = u.user_id
                WHERE i.created_at BETWEEN %s AND %s
                ORDER BY i.created_at DESC
                """,
                (start_date, end_date),
            )
            issues = cur.fetchall()

        def clean_text(text, max_length=300):
            """Очистка текста"""
            if not text:
                return ""
            try:
                text = str(text).strip()
                text = re.sub(r'[^\w\sА-Яа-яЁё.,-]', '', text)
                return text[:max_length]
            except Exception as e:
                logger.error(f"Error cleaning text: {e}")
                return str(text)[:max_length]

        # Заголовок
        pdf.add_page()
        pdf.set_font("DejaVuSans", "B", 16)
        pdf.cell(0, 10, txt="Отчет по заявкам ЖК", ln=1, align="C")
        pdf.set_font("DejaVuSans", "", 12)
        pdf.cell(0, 10, txt=f"Период: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}", ln=1, align="C")
        pdf.ln(10)

        # Параметры таблицы
        col_widths = [35, 35, 60, 20, 25, 30]
        headers = ["ФИО", "Адрес", "Описание", "Тип", "Статус", "Закрыл"]
        line_height = 6
        page_height = 270  # высота A4 без нижнего отступа

        def draw_table_header():
            pdf.set_font("DejaVuSans", "B", 10)
            x_start = pdf.get_x()
            y_start = pdf.get_y()
            for i, header in enumerate(headers):
                pdf.set_xy(x_start, y_start)
                pdf.multi_cell(col_widths[i], line_height, header, border=1, align="C")
                x_start += col_widths[i]
            pdf.set_y(y_start + line_height)
            pdf.set_font("DejaVuSans", "", 10)

        # Добавление страницы и заголовка таблицы
        pdf.add_page()
        draw_table_header()

        for issue in issues:
            data = [
                clean_text(issue[0]),
                clean_text(issue[1]),
                clean_text(issue[2]),
                "Сроч" if str(issue[3]).lower() == "urgent" else "Обыч",
                "выполнено" if str(issue[4]).lower() == "completed" else "новый",
                clean_text(issue[5])
            ]

            # Подсчет количества строк для каждой ячейки
            cell_lines = []
            for i, text in enumerate(data):
                lines = pdf.multi_cell(col_widths[i], line_height, text, border=0, align='L', split_only=True)
                cell_lines.append(len(lines))
            max_lines = max(cell_lines)
            row_height = max_lines * line_height

            # Проверка на переход страницы
            if pdf.get_y() + row_height > page_height:
                pdf.add_page()
                draw_table_header()

            # Отрисовка строки таблицы
            x_start = pdf.get_x()
            y_start = pdf.get_y()
            for i, text in enumerate(data):
                pdf.set_xy(x_start, y_start)
                pdf.multi_cell(col_widths[i], line_height, text, border=1, align='L')
                x_start += col_widths[i]
                pdf.set_xy(x_start, y_start)
            pdf.set_y(y_start + row_height)

        # Сохранение PDF в память
        pdf_bytes = BytesIO()
        pdf.output(pdf_bytes)
        pdf_bytes.seek(0)
        logger.info("PDF report generated successfully")
        return pdf_bytes

    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        raise Exception(f"Database error: {e}")
    except Exception as e:
        logger.error(f"Error generating PDF: {e}")
        raise
    finally:
        if conn:
            conn.close()
            
async def generate_and_send_report(
    update: Update, context: ContextTypes.DEFAULT_TYPE, start_date: datetime, end_date: datetime
):
    """Generate and send PDF report."""
    processing_msg = await update.effective_chat.send_message("🔄 Генерация отчета...")
    try:
        # Генерируем PDF
        pdf_bytes = generate_pdf_report(start_date, end_date)
        
        # Создаем временный файл в памяти
        pdf_file = BytesIO()
        pdf_file.write(pdf_bytes.getvalue())
        pdf_file.seek(0)
        pdf_file.name = f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        
        # Отправляем документ
        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=pdf_file,
            filename=pdf_file.name,
            caption=f"📊 Отчет за период с {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')}",
        )
        
        # Закрываем файлы
        pdf_bytes.close()
        pdf_file.close()
        
        await processing_msg.delete()
        await start(update, context)
        
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        await processing_msg.edit_text(f"❌ Ошибка генерации отчета: {str(e)}")

async def shutdown_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Initiate bot shutdown with confirmation."""
    if not await is_admin(update.effective_user.id):
        await update.callback_query.answer("❌ Доступ запрещен", show_alert=True)
        return
    keyboard = [
        [InlineKeyboardButton("✅ Да, остановить", callback_data="confirm_shutdown")],
        [InlineKeyboardButton("❌ Нет, отмена", callback_data="cancel_shutdown")],
    ]
    await send_and_remember(
        update,
        context,
        "⚠️ Вы уверены, что хотите остановить бота?",
        InlineKeyboardMarkup(keyboard),
    )

async def message_user(
    update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int
):
    """Initiate messaging to a user."""
    context.user_data["messaging_user_id"] = user_id
    await send_and_remember(
        update,
        context,
        "✍️ Введите сообщение для пользователя:",
        InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="back_to_main")]]),
    )
    context.user_data["awaiting_user_message"] = True

async def send_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send message to a user."""
    if "messaging_user_id" not in context.user_data:
        await send_and_remember(
            update,
            context,
            "❌ Ошибка: не найден пользователь.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
        return
    try:
        message = update.message.text
        user_id = context.user_data["messaging_user_id"]
        await context.bot.send_message(
            chat_id=user_id, text=f"✉️ Сообщение от поддержки:\n\n{message}"
        )
        await send_and_remember(
            update,
            context,
            "✅ Сообщение отправлено!",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
        context.user_data.pop("messaging_user_id", None)
        context.user_data.pop("awaiting_user_message", None)
    except Exception as e:
        logger.error(f"Error sending message: {e}")
        await send_and_remember(
            update,
            context,
            "❌ Не удалось отправить сообщение. Пользователь, возможно, не начал диалог с ботом.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle button callbacks."""
    query = update.callback_query
    if query.data == "do_nothing":
        return
    if not query:
        logger.error("No callback query received")
        return
    await query.answer()
    user_id = update.effective_user.id
    role = await get_user_role(user_id)
    logger.info(f"Processing button: {query.data} for user {user_id}")
    try:
        if query.data == "start":
            await start(update, context)
        elif query.data == "select_resident":
            await select_user_type(update, context, USER_TYPES["resident"])
        elif query.data == "select_potential_buyer":
            await select_user_type(update, context, USER_TYPES["potential_buyer"])
        elif query.data == "complex_info":
            await show_complex_info(update, context)
        elif query.data == "pricing_info":
            await show_pricing_info(update, context)
        elif query.data == "sales_team":
            await show_sales_team(update, context)
        elif query.data == "ask_sales_question":
            await ask_sales_question(update, context)
        elif query.data == "add_resident":
            await add_resident(update, context)
        elif query.data == "delete_resident":
            await delete_resident(update, context)
        elif query.data == "new_request":
            await process_new_request(update, context)
        elif query.data == "my_requests":
            logger.info(f"User {user_id} pressed 'my_requests' button")
            await show_user_requests(update, context)
        elif query.data == "help":
            logger.info(f"User {user_id} pressed 'help' button")
            await show_help(update, context)
        elif query.data == "active_requests":
            await show_active_requests(update, context)
        elif query.data == "urgent_requests":
            await show_urgent_requests(update, context)
        elif query.data == "completed_requests":
            await completed_requests(update, context)
        elif query.data == "reports_menu":
            keyboard = [
                [InlineKeyboardButton("📅 Последние 7 дней", callback_data="report_7")],
                [InlineKeyboardButton("📅 Последние 30 дней", callback_data="report_30")],
                [InlineKeyboardButton("📅 Текущий месяц", callback_data="report_month")],
                [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")],
            ]
            await send_and_remember(
                update,
                context,
                "📊 Выберите период отчета:",
                InlineKeyboardMarkup(keyboard),
            )
        elif query.data == "manage_agents":
            await manage_agents_menu(update, context)
        elif query.data == "shutdown_bot":
            await shutdown_bot(update, context)
        elif query.data == "confirm_shutdown":
            await confirm_shutdown(update, context)
        elif query.data == "cancel_shutdown":
            await start(update, context)
        elif query.data.startswith("report_"):
            await process_report_period(update, context, query.data.split("_")[1])
        elif query.data.startswith("request_detail_"):
            issue_id = int(query.data.split("_")[2])
            await show_request_detail(update, context, issue_id)
        elif query.data.startswith("complete_request_"):
            issue_id = int(query.data.split("_")[2])
            await complete_request(update, context, issue_id)
        elif query.data.startswith("message_user_"):
            user_id = int(query.data.split("_")[2])
            await message_user(update, context, user_id)
        elif query.data.startswith("agent_info_"):
            user_id = int(query.data.split("_")[2])
            await show_agent_info(update, context, user_id)
        elif query.data.startswith("delete_agent_"):
            user_id = int(query.data.split("_")[2])
            await delete_agent(update, context, user_id)
        elif query.data == "add_agent":
            await add_agent(update, context)
        elif query.data in ["cancel", "back_to_main"]:
            await start(update, context)
        else:
            logger.warning(f"Unknown command: {query.data}")
            await send_and_remember(
                update,
                context,
                "⚠️ Команда не распознана",
                main_menu_keyboard(user_id, role, user_type=context.user_data.get("user_type")),
            )
    except psycopg2.Error as e:
        logger.error(f"Database error in button_handler: {e}")
        await send_and_remember(
            update,
            context,
            f"❌ Ошибка базы данных: {e}",
            main_menu_keyboard(user_id, role, user_type=context.user_data.get("user_type")),
        )
    except Exception as e:
        logger.error(f"Unexpected error in button_handler: {e}")
        await send_and_remember(
            update,
            context,
            f"❌ Ошибка: {e}",
            main_menu_keyboard(user_id, role, user_type=context.user_data.get("user_type")),
        )

async def show_agent_info(
    update: Update, context: ContextTypes.DEFAULT_TYPE, agent_id: int
):
    """Show agent information."""
    if not await is_admin(update.effective_user.id):
        await update.callback_query.answer("❌ Доступ запрещен", show_alert=True)
        return
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT user_id, username, full_name, role, registration_date
                FROM users
                WHERE user_id = %s
                """,
                (agent_id,),
            )
            agent = cur.fetchone()

        if not agent:
            await update.callback_query.answer("Агент не найден", show_alert=True)
            return

        role_text = (
            "Администратор" if agent[3] == SUPPORT_ROLES["admin"] else "Агент поддержки"
        )
        text = (
            f"👤 Информация о сотруднике:\n\n"
            f"🆔 ID: {agent[0]}\n"
            f"👤 Имя: {agent[2]}\n"
            f"📱 Username: @{agent[1] or 'нет'}\n"
            f"🏅 Роль: {role_text}\n"
            f"📅 Дата регистрации: {agent[4].strftime('%d.%m.%Y')}"
        )
        keyboard = [
            [InlineKeyboardButton("🔙 Назад", callback_data="manage_agents")],
            [InlineKeyboardButton("❌ Удалить", callback_data=f"delete_agent_{agent[0]}")],
        ]
        await send_and_remember(
            update,
            context,
            text,
            InlineKeyboardMarkup(keyboard),
        )
    except psycopg2.Error as e:
        logger.error(f"Error retrieving agent info: {e}")
        await send_and_remember(
            update,
            context,
            "❌ Ошибка при получении данных.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
    finally:
        if conn:
            conn.close()

async def delete_agent(
    update: Update, context: ContextTypes.DEFAULT_TYPE, agent_id: int
):
    """Delete an agent."""
    if not await is_admin(update.effective_user.id):
        await update.callback_query.answer("❌ Доступ запрещен", show_alert=True)
        return
    if agent_id == update.effective_user.id:
        await update.callback_query.answer("❌ Нельзя удалить самого себя", show_alert=True)
        return
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM users WHERE user_id = %s", (agent_id,))
            conn.commit()
        await update.callback_query.answer("✅ Агент удален", show_alert=True)
        await manage_agents_menu(update, context)
    except psycopg2.Error as e:
        logger.error(f"Error deleting agent: {e}")
        await update.callback_query.answer("❌ Ошибка при удалении агента", show_alert=True)
    finally:
        if conn:
            conn.close()

async def add_agent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Initiate adding a new agent."""
    if not await is_admin(update.effective_user.id):
        await update.callback_query.answer("❌ Доступ запрещен", show_alert=True)
        return
    await send_and_remember(
        update,
        context,
        "✍️ Введите Telegram ID нового агента:",
        InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="manage_agents")]]),
    )
    context.user_data["awaiting_agent_id"] = True

async def process_new_agent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process new agent ID with validation."""
    agent_id_text = update.message.text.strip()
    if not re.match(r"^\d{5,20}$", agent_id_text):
        await send_and_remember(
            update,
            context,
            "❌ Неверный формат ID. Введите числовой Telegram ID (5-20 цифр):",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="manage_agents")]]),
        )
        return
    try:
        agent_id = int(agent_id_text)
        context.user_data["new_agent_id"] = agent_id
        context.user_data.pop("awaiting_agent_id", None)
        await send_and_remember(
            update,
            context,
            "✍️ Введите полное имя нового агента:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="manage_agents")]]),
        )
        context.user_data["awaiting_agent_name"] = True
    except ValueError:
        await send_and_remember(
            update,
            context,
            "❌ Неверный формат ID. Введите числовой Telegram ID:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="manage_agents")]]),
        )

async def manage_agents_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show manage agents menu."""
    if not await is_admin(update.effective_user.id):
        await update.callback_query.answer("❌ Доступ запрещен", show_alert=True)
        return
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT user_id, full_name FROM users WHERE role = %s", (SUPPORT_ROLES["agent"],))
            agents = cur.fetchall()

        if not agents:
            await send_and_remember(
                update,
                context,
                "👥 Нет зарегистрированных агентов.",
                InlineKeyboardMarkup([[InlineKeyboardButton("➕ Добавить агента", callback_data="add_agent")],
                                     [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]),
            )
            return

        keyboard = [
            [InlineKeyboardButton(f"👤 {agent[1]} (ID: {agent[0]})", callback_data=f"agent_info_{agent[0]}")]
            for agent in agents
        ]
        keyboard.append([InlineKeyboardButton("➕ Добавить агента", callback_data="add_agent")])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")])

        await send_and_remember(
            update,
            context,
            "👥 Управление персоналом:",
            InlineKeyboardMarkup(keyboard),
        )
    except psycopg2.Error as e:
        logger.error(f"Error retrieving agents: {e}")
        await send_and_remember(
            update,
            context,
            "❌ Ошибка при получении данных.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
    finally:
        if conn:
            conn.close()

async def show_complex_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show information about the residential complex."""
    if context.user_data.get("user_type") != USER_TYPES["potential_buyer"]:
        await update.callback_query.answer("❌ Доступ запрещен", show_alert=True)
        return
    text = (
        "🏠 Информация о ЖК Сункар:\n\n"
        "ЖК Сункар – современный жилой комплекс с развитой инфраструктурой.\n"
        "📍 Расположение: г. Алматы, ул. Примерная, 123\n"
        "🌳 Особенности: зеленые зоны, детские площадки, паркинг\n"
        "🏬 Типы квартир: 1, 2, 3-комнатные\n"
        "📞 Контакт: @SunqarSales"
    )
    await send_and_remember(
        update,
        context,
        text,
        main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id), user_type=USER_TYPES["potential_buyer"]),
    )

async def show_pricing_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show pricing information per square meter."""
    if context.user_data.get("user_type") != USER_TYPES["potential_buyer"]:
        await update.callback_query.answer("❌ Доступ запрещен", show_alert=True)
        return
    text = (
        "💰 Цена за квадратный метр в ЖК Сункар:\n\n"
        "• 1-комнатные: 300,000 KZT/м²\n"
        "• 2-комнатные: 280,000 KZT/м²\n"
        "• 3-комнатные: 270,000 KZT/м²\n\n"
        "📞 Для точной стоимости свяжитесь с отделом продаж: @SunqarSales"
    )
    await send_and_remember(
        update,
        context,
        text,
        main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id), user_type=USER_TYPES["potential_buyer"]),
    )

async def show_sales_team(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show sales team contact information and option to ask a question."""
    if context.user_data.get("user_type") != USER_TYPES["potential_buyer"]:
        await update.callback_query.answer("❌ Доступ запрещен", show_alert=True)
        return
    text = (
        "👥 Отдел продаж ЖК Сункар:\n\n"
        "1. Иван Иванов – @IvanSales – +7 777 123 4567\n"
        "2. Анна Смирнова – @AnnaSales – +7 777 987 6543\n\n"
        "📞 Свяжитесь напрямую или задайте вопрос здесь:"
    )
    keyboard = [
        [InlineKeyboardButton("✍️ Задать вопрос", callback_data="ask_sales_question")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")],
    ]
    await send_and_remember(
        update,
        context,
        text,
        InlineKeyboardMarkup(keyboard),
    )

async def ask_sales_question(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Prompt user to ask a sales question."""
    if context.user_data.get("user_type") != USER_TYPES["potential_buyer"]:
        await update.callback_query.answer("❌ Доступ запрещен", show_alert=True)
        return
    await send_and_remember(
        update,
        context,
        "✍️ Введите ваш вопрос для отдела продаж:",
        InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="back_to_main")]]),
    )
    context.user_data["awaiting_sales_question"] = True

async def process_sales_question(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process and forward sales question to the sales team."""
    if "awaiting_sales_question" not in context.user_data:
        await send_and_remember(
            update,
            context,
            "❌ Ошибка: не ожидается вопрос.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id), user_type=USER_TYPES["potential_buyer"]),
        )
        return
    question = update.message.text
    user = update.effective_user
    try:
        # Forward question to sales team (e.g., director or sales channel)
        await context.bot.send_message(
            chat_id=DIRECTOR_CHAT_ID,  # Or replace with a sales team chat ID
            text=(
                f"❓ Новый вопрос от потенциального покупателя:\n\n"
                f"👤 От: {user.full_name} (@{user.username or 'нет'})\n"
                f"🆔 ID: {user.id}\n"
                f"📝 Вопрос: {question}\n"
                f"🕒 Время: {datetime.now().strftime('%H:%M %d.%m.%Y')}"
            ),
        )
        await send_and_remember(
            update,
            context,
            "✅ Ваш вопрос отправлен в отдел продаж! Ожидайте ответа.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id), user_type=USER_TYPES["potential_buyer"]),
        )
    except Exception as e:
        logger.error(f"Error forwarding sales question: {e}")
        await send_and_remember(
            update,
            context,
            "❌ Ошибка при отправке вопроса. Попробуйте позже.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id), user_type=USER_TYPES["potential_buyer"]),
        )
    finally:
        context.user_data.pop("awaiting_sales_question", None)

async def delete_resident(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Prompt admin to enter chat ID of resident to delete."""
    user_id = update.effective_user.id
    role = await get_user_role(user_id)
    if role != SUPPORT_ROLES["admin"] and user_id != DIRECTOR_CHAT_ID:
        await update.callback_query.answer("❌ Доступ запрещен", show_alert=True)
        return
    await send_and_remember(
        update,
        context,
        "🗑 Введите chat ID резидента для удаления:",
        InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="back_to_main")]]),
    )
    context.user_data["awaiting_resident_id_delete"] = True

async def process_resident_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаление резидента с улучшенной обработкой ошибок и каскадным удалением."""
    try:
        chat_id = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Неверный формат ID. Введите числовой chat ID.")
        return

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # 1. Проверяем существование резидента
            cur.execute("SELECT resident_id, full_name FROM residents WHERE chat_id = %s", (chat_id,))
            resident = cur.fetchone()
            
            if not resident:
                await update.message.reply_text(f"❌ Резидент с chat ID {chat_id} не найден.")
                return

            resident_id, full_name = resident

            # 2. Проверяем количество связанных заявок и логов перед удалением
            cur.execute("SELECT COUNT(*) FROM issues WHERE resident_id = %s", (resident_id,))
            issue_count_before = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM issue_logs WHERE issue_id IN (SELECT issue_id FROM issues WHERE resident_id = %s)", (resident_id,))
            log_count_before = cur.fetchone()[0]
            logger.info(f"Найдено {issue_count_before} заявок и {log_count_before} логов для resident_id {resident_id} перед удалением")

            # 3. Удаляем резидента (каскадное удаление обработает issues и issue_logs)
            cur.execute("DELETE FROM residents WHERE resident_id = %s", (resident_id,))
            conn.commit()

            # 4. Удаляем запись из users независимо от роли
            cur.execute("DELETE FROM users WHERE user_id = %s", (chat_id,))
            conn.commit()

            # 5. Проверяем количество оставшихся записей после каскада
            cur.execute("SELECT COUNT(*) FROM issues WHERE resident_id = %s", (resident_id,))
            issue_count_after = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM issue_logs WHERE issue_id IN (SELECT issue_id FROM issues WHERE resident_id = %s)", (resident_id,))
            log_count_after = cur.fetchone()[0]
            issues_deleted = issue_count_before - issue_count_after
            logs_deleted = log_count_before - log_count_after
            logger.info(f"Удалено {issues_deleted} заявок и {logs_deleted} логов каскадно для resident_id {resident_id}")

            # Успешное сообщение с информацией о каскадном удалении
            await update.message.reply_text(
                f"✅ Резидент {full_name} (ID: {chat_id}) успешно удалён.\n"
                f"Удалено заявок: {issues_deleted}, логов: {logs_deleted}",
                reply_markup=main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id))
            )

    except psycopg2.Error as e:
        logger.error(f"Database error: {e.pgerror if hasattr(e, 'pgerror') else str(e)}")
        await update.message.reply_text(
            f"❌ Ошибка базы данных: {e.pgerror if hasattr(e, 'pgerror') else str(e)}",
            reply_markup=main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id))
        )
        if conn:
            conn.rollback()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        await update.message.reply_text(
            f"❌ Непредвиденная ошибка: {str(e)}",
            reply_markup=main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id))
        )
    finally:
        if conn:
            conn.close()
        context.user_data.pop("awaiting_resident_id_delete", None)
        
async def add_resident(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Prompt admin to enter chat ID of new resident."""
    user_id = update.effective_user.id
    role = await get_user_role(user_id)
    if role != SUPPORT_ROLES["admin"] and user_id != DIRECTOR_CHAT_ID:
        await update.callback_query.answer("❌ Доступ запрещен", show_alert=True)
        return
    await send_and_remember(
        update,
        context,
        "🏠 Введите chat ID нового резидента:",
        InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="back_to_main")]]),
    )
    context.user_data["awaiting_resident_id_add"] = True

async def process_resident_id_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process chat ID for new resident and prompt for name with enhanced validation and state management."""
    if "awaiting_resident_id_add" not in context.user_data:
        await send_and_remember(
            update,
            context,
            "❌ Ошибка: не ожидается ввод chat ID.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id), user_type=context.user_data.get("user_type")),
        )
        return

    chat_id_input = update.message.text.strip()
    logger.info(f"Received raw chat ID input for new resident: '{chat_id_input}' (length: {len(chat_id_input)}, type: {type(chat_id_input)})")
    logger.info(f"Full update message: {update.message.to_dict()}")

    try:
        # Sanitize input by removing any non-digit characters
        sanitized_input = re.sub(r'[^\d]', '', chat_id_input)
        logger.info(f"Sanitized chat ID input: '{sanitized_input}' (length: {len(sanitized_input)})")
        if not sanitized_input:
            raise ValueError("No valid digits found in input")
        chat_id = int(sanitized_input)
        if chat_id <= 0:
            raise ValueError("Chat ID must be a positive number")
        context.user_data["new_resident_chat_id"] = chat_id

        # Check if already a resident
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT chat_id FROM residents WHERE chat_id = %s", (chat_id,))
                if cur.fetchone():
                    await send_and_remember(
                        update,
                        context,
                        f"❌ Пользователь с chat ID {chat_id} уже зарегистрирован как резидент.",
                        main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id), user_type=context.user_data.get("user_type")),
                    )
                    return
        finally:
            conn.close()

        # Transition to awaiting full name
        await send_and_remember(
            update,
            context,
            "👤 Введите ФИО резидента:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="back_to_main")]]),
        )
        context.user_data.pop("awaiting_resident_id_add", None)  # Clear the old state
        context.user_data["awaiting_new_resident_name"] = True
    except ValueError as e:
        logger.error(f"Invalid chat ID format: '{chat_id_input}', sanitized: '{sanitized_input}', error: {e}")
        await send_and_remember(
            update,
            context,
            "❌ Неверный формат chat ID. Введите положительное число (например, 123456789). Проверьте, нет ли скрытых символов. Лог: " + str(e),
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="back_to_main")]]),
        )
    except psycopg2.Error as e:
        logger.error(f"Database error checking resident: {e}")
        await send_and_remember(
            update,
            context,
            "❌ Ошибка базы данных. Попробуйте позже.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id), user_type=context.user_data.get("user_type")),
        )
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            
async def process_new_resident_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process full name for new resident and prompt for address."""
    if "awaiting_new_resident_name" not in context.user_data:
        await send_and_remember(
            update,
            context,
            "❌ Ошибка: не ожидается ввод ФИО.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id), user_type=context.user_data.get("user_type")),
        )
        return

    full_name = update.message.text.strip()
    logger.info(f"Received full name for new resident: '{full_name}' (chat_id: {context.user_data.get('new_resident_chat_id')})")
    context.user_data["new_resident_name"] = full_name

    # Proceed to next step (address)
    await send_and_remember(
        update,
        context,
        "🏠 Введите адрес резидента:",
        InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="back_to_main")]]),
    )
    context.user_data["awaiting_new_resident_address"] = True
    context.user_data.pop("awaiting_new_resident_name", None)

async def process_new_resident_address(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process address for new resident and prompt for phone."""
    if "awaiting_new_resident_address" not in context.user_data:
        await send_and_remember(
            update,
            context,
            "❌ Ошибка: не ожидается ввод адреса.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id), user_type=context.user_data.get("user_type")),
        )
        return
    context.user_data["new_resident_address"] = update.message.text
    await send_and_remember(
        update,
        context,
        "📞 Введите номер телефона резидента:",
        InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="back_to_main")]]),
    )
    context.user_data.pop("awaiting_new_resident_address", None)
    context.user_data["awaiting_new_resident_phone"] = True

async def process_new_resident_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Save new resident to database and update user_type with robust notification handling."""
    if "awaiting_new_resident_phone" not in context.user_data:
        await send_and_remember(
            update,
            context,
            "❌ Ошибка: не ожидается ввод телефона.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id), user_type=context.user_data.get("user_type")),
        )
        return

    # Validate required data
    required_keys = ["new_resident_chat_id", "new_resident_name", "new_resident_address"]
    missing_keys = [key for key in required_keys if key not in context.user_data]
    if missing_keys:
        await send_and_remember(
            update,
            context,
            f"❌ Отсутствуют данные: {', '.join(missing_keys)}. Начните заново.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id), user_type=context.user_data.get("user_type")),
        )
        return

    phone = update.message.text.strip()
    chat_id = context.user_data["new_resident_chat_id"]
    full_name = context.user_data["new_resident_name"]
    address = context.user_data["new_resident_address"]
    admin_user_id = update.effective_user.id
    admin_role = await get_user_role(admin_user_id)

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Check for existing resident
            cur.execute("SELECT resident_id FROM residents WHERE chat_id = %s", (chat_id,))
            if cur.fetchone():
                await send_and_remember(
                    update,
                    context,
                    f"❌ Пользователь с chat ID {chat_id} уже зарегистрирован как резидент.",
                    main_menu_keyboard(admin_user_id, admin_role, user_type=context.user_data.get("user_type")),
                )
                return

            # Insert into residents table
            cur.execute(
                """
                INSERT INTO residents (chat_id, full_name, address, phone, registration_date)
                VALUES (%s, %s, %s, %s, %s) RETURNING resident_id
                """,
                (chat_id, full_name, address, phone, datetime.now()),
            )
            resident_id = cur.fetchone()[0]

            # Insert or update users table for the resident
            username = update.effective_user.username if update.effective_user.username else None
            cur.execute(
                """
                INSERT INTO users (user_id, username, full_name, role, registration_date)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username, full_name = EXCLUDED.full_name, role = EXCLUDED.role, registration_date = EXCLUDED.registration_date
                """,
                (chat_id, username, full_name, SUPPORT_ROLES["user"], datetime.now()),
            )
            conn.commit()

            # Attempt to notify the new resident
            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="🏠 Вы зарегистрированы как резидент ЖК Сункар! Используйте /start для доступа к меню.",
                )
                logger.info(f"Successfully notified new resident (chat_id: {chat_id})")
            except telegram.error.BadRequest as e:
                logger.warning(f"Failed to notify new resident (chat_id: {chat_id}): {e}")
                await send_and_remember(
                    update,
                    context,
                    f"⚠️ Не удалось уведомить резидента (chat ID: {chat_id}). Убедитесь, что пользователь запустил бота с /start.",
                    main_menu_keyboard(admin_user_id, admin_role, user_type=context.user_data.get("user_type")),
                )

            # Send success message to admin
            await send_and_remember(
                update,
                context,
                f"✅ Резидент {full_name} (chat ID: {chat_id}) добавлен с ID {resident_id}.",
                main_menu_keyboard(admin_user_id, admin_role, user_type=context.user_data.get("user_type")),
            )
    except psycopg2.Error as e:
        logger.error(f"Database error adding resident (chat_id={chat_id}): {e}")
        await send_and_remember(
            update,
            context,
            "❌ Ошибка базы данных. Попробуйте позже.",
            main_menu_keyboard(admin_user_id, admin_role, user_type=context.user_data.get("user_type")),
        )
        conn.rollback()
    finally:
        context.user_data.pop("awaiting_new_resident_phone", None)
        context.user_data.pop("new_resident_chat_id", None)
        context.user_data.pop("new_resident_name", None)
        context.user_data.pop("new_resident_address", None)
        conn.close()

async def save_user_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text messages based on context."""
    logger.info(f"Processing text input from user {update.effective_user.id}: {update.message.text}")
    logger.info(f"Current context.user_data: {context.user_data}")
    if "awaiting_problem" in context.user_data:
        logger.info(f"Processing problem report for user {update.effective_user.id}")
        await process_problem_report(update, context)
    elif "awaiting_name" in context.user_data:
        logger.info(f"Processing name for user {update.effective_user.id}")
        await process_user_name(update, context)
    elif "awaiting_address" in context.user_data:
        logger.info(f"Processing address for user {update.effective_user.id}")
        await process_user_address(update, context)
    elif "awaiting_phone" in context.user_data:
        logger.info(f"Processing phone for user {update.effective_user.id}")
        await process_user_phone(update, context)
    elif "awaiting_solution" in context.user_data:
        logger.info(f"Processing solution for user {update.effective_user.id}")
        await save_solution(update, context)
    elif "awaiting_agent_id" in context.user_data:
        logger.info(f"Processing agent ID for user {update.effective_user.id}")
        await process_new_agent(update, context)
    elif "awaiting_agent_name" in context.user_data:
        logger.info(f"Processing agent name for user {update.effective_user.id}")
        await save_agent(update, context)
    elif "awaiting_user_message" in context.user_data:
        logger.info(f"Processing user message for user {update.effective_user.id}")
        await send_user_message(update, context)
    elif "awaiting_sales_question" in context.user_data:
        logger.info(f"Processing sales question for user {update.effective_user.id}")
        await process_sales_question(update, context)
    elif "awaiting_resident_id_delete" in context.user_data:
        logger.info(f"Processing resident deletion for user {update.effective_user.id}")
        await process_resident_delete(update, context)
    elif "awaiting_resident_id_add" in context.user_data:
        logger.info(f"Processing new resident ID for user {update.effective_user.id}")
        await process_resident_id_add(update, context)
    elif "awaiting_new_resident_name" in context.user_data:
        logger.info(f"Processing new resident name for user {update.effective_user.id}")
        await process_new_resident_name(update, context)
    elif "awaiting_new_resident_address" in context.user_data:
        logger.info(f"Processing new resident address for user {update.effective_user.id}")
        await process_new_resident_address(update, context)
    elif "awaiting_new_resident_phone" in context.user_data:
        logger.info(f"Processing new resident phone for user {update.effective_user.id}")
        await process_new_resident_phone(update, context)
    else:
        logger.warning(f"No awaiting state for user {update.effective_user.id}")
        await send_and_remember(
            update,
            context,
            "⚠️ Неизвестная команда. Используйте кнопки меню.",
            main_menu_keyboard(
                update.effective_user.id,
                await get_user_role(update.effective_user.id),
                user_type=context.user_data.get("user_type")
            ),
        )

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors."""
    error = context.error
    if isinstance(error, (NetworkError, TimedOut)):
        logger.warning(f"⚠️ Network error occurred: {error}. Attempting to reconnect...")
        if update and update.effective_user:
            await send_and_remember(
                update,
                context,
                "⚠️ Проблема с сетью. Пожалуйста, попробуйте позже.",
                main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
            )
        return
    logger.error("Exception occurred:", exc_info=context.error)
    if update and update.effective_user:
        await send_and_remember(
            update,
            context,
            "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже или обратитесь в техподдержку.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            try:
                conn = get_db_connection()
                conn.close()
                self.wfile.write(b'OK DB OK')
            except Exception as e:
                self.wfile.write(f'DB ERROR: {str(e)}'.encode())
        else:
            self.send_response(404)
            self.end_headers()

def run_health_check():
    port = int(os.getenv("PORT", 8080))
    server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
    logger.info(f"✅ Health check server running on port {port} (PID: {os.getpid()})")
    server.serve_forever()

def start_health_server():
    server_thread = Thread(target=run_health_check, daemon=True)
    server_thread.start()
    time.sleep(5)
    return server_thread

async def generate_report_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /report command to initiate report generation."""
    user_id = update.effective_user.id
    role = await get_user_role(user_id)
    if role < SUPPORT_ROLES["admin"]:
        await send_and_remember(
            update,
            context,
            "❌ Доступ запрещен. Только администраторы могут генерировать отчеты.",
            main_menu_keyboard(user_id, role),
        )
        return
    
    keyboard = [
        [InlineKeyboardButton("📅 Последние 7 дней", callback_data="report_7")],
        [InlineKeyboardButton("📅 Последние 30 дней", callback_data="report_30")],
        [InlineKeyboardButton("📅 Текущий месяц", callback_data="report_month")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")],
    ]
    await send_and_remember(
        update,
        context,
        "📊 Выберите период отчета:",
        InlineKeyboardMarkup(keyboard),
    )

# Remove the standalone application.add_handler line
# Update the main() function (near the end of the file) as follows:
def main() -> None:
    """Run the bot with auto-restart."""
    init_db()

    while True:
        try:
            health_server = start_health_server()
            logger.info("🔄 Initializing bot...")
            application = Application.builder().token(TELEGRAM_TOKEN).build()

            # Add all handlers here
            application.add_handler(CommandHandler("start", start))
            application.add_handler(CommandHandler("report", generate_report_command))
            application.add_handler(CommandHandler("clear", clear_chat))
            logger.info("✅ Registered CallbackQueryHandler for button_handler")
            application.add_handler(CallbackQueryHandler(button_handler))
            application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, save_user_data))
            application.add_error_handler(error_handler)

            logger.info("🚀 Starting bot polling...")
            application.run_polling(
                drop_pending_updates=True,
                close_loop=False,
                allowed_updates=Update.ALL_TYPES
            )
        except KeyboardInterrupt:
            logger.info("🛑 Bot stopped by user")
            break
        except Exception as e:
            logger.error(f"⚠️ Bot crashed: {str(e)[:200]}")
            logger.info("🔄 Restarting in 10 seconds...")
            time.sleep(10)
            
if __name__ == '__main__':
    logger.info("🛠 Starting application...")
    time.sleep(8)
    main()