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
SUPPORT_ROLES = {"user": 1, "agent": 2, "admin": 3, "resident": 4}
USER_TYPES = {"resident": "resident", "potential_buyer": "potential_buyer"}
def init_db():
    """Initialize database tables if they don't exist."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Создание таблицы users с user_type
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    full_name TEXT NOT NULL,
                    role INTEGER NOT NULL,
                    user_type VARCHAR(50),  -- Added user_type column
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
            
            # Создание таблицы issues
            cur.execute("""
                CREATE TABLE IF NOT EXISTS issues (
                    issue_id SERIAL PRIMARY KEY,
                    resident_id INTEGER NOT NULL REFERENCES residents(resident_id),
                    description TEXT NOT NULL,
                    category TEXT NOT NULL,
                    status TEXT NOT NULL,
                    solution TEXT,
                    created_at TIMESTAMP NOT NULL,
                    completed_at TIMESTAMP,
                    closed_by BIGINT REFERENCES users(user_id)
                )
            """)
            
            # Создание таблицы issue_logs
            cur.execute("""
                CREATE TABLE IF NOT EXISTS issue_logs (
                    log_id SERIAL PRIMARY KEY,
                    issue_id INTEGER NOT NULL REFERENCES issues(issue_id),
                    action TEXT NOT NULL,
                    user_id BIGINT NOT NULL REFERENCES users(user_id),
                    details TEXT,
                    action_time TIMESTAMP NOT NULL
                )
            """)
            
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
    if str(user_id) == DIRECTOR_CHAT_ID:
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO users (user_id, username, full_name, role, user_type, registration_date)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE SET role = EXCLUDED.role, user_type = EXCLUDED.user_type
                    """,
                    (user_id, None, "Director", SUPPORT_ROLES["admin"], None, datetime.now(timezone.utc))
                )
                conn.commit()
                logger.info(f"Auto-registered director {user_id} as admin")
                return SUPPORT_ROLES["admin"]
        except psycopg2.Error as e:
            logger.error(f"Database error auto-registering director {user_id}: {e}", exc_info=True)
            return SUPPORT_ROLES["admin"]  # Return admin role even on error for director
        finally:
            if conn:
                conn.close()

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT role FROM users WHERE user_id = %s", (user_id,))
            result = cur.fetchone()
            if result:
                return result[0]  # Return role as integer
            # Insert new user with default role if not found
            cur.execute(
                """
                INSERT INTO users (user_id, username, full_name, role, user_type, registration_date)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id) DO NOTHING
                """,
                (user_id, None, "Unknown", SUPPORT_ROLES["user"], None, datetime.now(timezone.utc))
            )
            conn.commit()
            return SUPPORT_ROLES["user"]  # Default to user role (1)
    except psycopg2.Error as e:
        logger.error(f"Database error getting role for user_id {user_id}: {e}", exc_info=True)
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
    except Exception as e:
        logger.error(f"Unexpected error deleting message: {e}")
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
    
    # Удаляем предыдущие сообщения с обработкой ошибок
    try:
        await delete_previous_messages(update, context)
    except Exception as e:
        logger.warning(f"Error deleting previous messages: {e}")
    
    retries = 3
    for attempt in range(retries):
        try:
            message = await update.effective_chat.send_message(
                text, reply_markup=reply_markup
            )
            context.user_data["last_message_id"] = message.message_id
            logger.info(f"Message sent, ID {message.message_id} stored for user {update.effective_user.id}")
            return message
        except telegram.error.BadRequest as e:
            if "Message to delete not found" in str(e):
                logger.warning("Message to delete not found, continuing")
                continue
            raise
        except (NetworkError, TimedOut) as e:
            logger.warning(f"Network error on attempt {attempt + 1}: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
                continue
            logger.error(f"Failed to send message after {retries} attempts: {e}")
            raise
        except Exception as e:
            logger.error(f"Error sending message to user {update.effective_user.id}: {e}")
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
    """Handle /clear command to fully reset chat history."""
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    
    try:
        context.user_data.clear()
        context.chat_data.clear()
        
        # Get the current message ID
        current_message_id = update.message.message_id
        
        # Delete recent messages (limit to 100 to avoid rate limits)
        message_ids = list(range(max(1, current_message_id - 100), current_message_id + 1))
        
        async def delete_single_message(msg_id):
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
                logger.info(f"Deleted message ID {msg_id} for user {user_id}")
                await asyncio.sleep(0.05)  # Small delay to avoid rate limits
            except telegram.error.BadRequest as e:
                if "message to delete not found" not in str(e).lower():
                    logger.warning(f"Failed to delete message ID {msg_id}: {e}")
            except telegram.error.RetryAfter as e:
                logger.warning(f"Rate limit hit: {e}. Waiting {e.retry_after} seconds")
                await asyncio.sleep(e.retry_after)
            except Exception as e:
                logger.warning(f"Error deleting message ID {msg_id}: {e}")
        
        # Delete messages in batches
        batch_size = 20
        for i in range(0, len(message_ids), batch_size):
            batch = message_ids[i:i + batch_size]
            await asyncio.gather(*[delete_single_message(msg_id) for msg_id in batch])
        
        # Send confirmation message
        await send_and_remember(
            update,
            context,
            "🧹 Чат полностью очищен! Нажмите /start, чтобы начать заново.",
            main_menu_keyboard(user_id, await get_user_role(user_id), user_type=context.user_data.get("user_type"))
        )
    except Exception as e:
        logger.error(f"Error clearing chat for user {user_id}: {e}")
        await send_and_remember(
            update,
            context,
            "❌ Не удалось полностью очистить чат. Попробуйте снова или используйте /start.",
            main_menu_keyboard(user_id, await get_user_role(user_id), user_type=context.user_data.get("user_type"))
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
    """Process user phone number."""
    if not context.user_data.get("registration_flow") or not context.user_data.get("awaiting_phone"):
        logger.warning(f"User {update.effective_user.id} sent phone number outside registration flow")
        await send_and_remember(
            update,
            context,
            "❌ Ошибка: вы не в процессе регистрации. Используйте /start.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
        return
    
    phone = update.message.text.strip()
    # Stricter phone validation
    cleaned_phone = re.sub(r"[^\d+]", "", phone)
    if not re.match(r"^\+?\d{10,15}$", cleaned_phone):
        await send_and_remember(
            update,
            context,
            "❌ Неверный формат телефона. Введите номер в формате +1234567890:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]),
        )
        return
    
    context.user_data["user_phone"] = cleaned_phone
    context.user_data["registration_flow"] = True
    context.user_data.pop("awaiting_phone", None)
    context.user_data["awaiting_problem"] = True
    logger.info(f"Stored user_phone: {cleaned_phone} for chat_id: {update.effective_user.id}")
    await send_and_remember(
        update,
        context,
        "✍️ Опишите вашу проблему:",
        InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]),
    )
            
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

def main_menu_keyboard(user_id: int, role: int, is_in_main_menu: bool = False, user_type: str = None) -> InlineKeyboardMarkup:
    """Generate the main menu keyboard based on user role and user_type."""
    keyboard = []

    # Fetch user_type from database if not provided (only if user exists)
    if user_type is None and role is not None:
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT user_type FROM users WHERE user_id = %s", (user_id,))
                result = cur.fetchone()
                user_type = result[0] if result else None
        except psycopg2.Error as e:
            logger.error(f"Database error fetching user_type for {user_id}: {e}", exc_info=True)
            user_type = None
        finally:
            if conn:
                conn.close()

    # New/unregistered users (no role or user_type)
    if role is None or (role == SUPPORT_ROLES["user"] and user_type is None):
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🏠 Зарегистрироваться как резидент", callback_data="register_as_resident")],
            [InlineKeyboardButton("🛒 Зарегистрироваться как покупатель", callback_data="select_potential_buyer")]
        ])

    # Admin menu (priority over user_type)
    if role == SUPPORT_ROLES["admin"]:
        keyboard = [
            [InlineKeyboardButton("📝 Добавить резидента", callback_data="add_resident")],
            [InlineKeyboardButton("🗑 Удалить резидента", callback_data="delete_resident")],
            [InlineKeyboardButton("👷 Управление сотрудниками", callback_data="manage_agents")],
            [InlineKeyboardButton("📊 Отчеты", callback_data="reports_menu")],
            [InlineKeyboardButton("🔔 Активные заявки", callback_data="active_requests")],
            [InlineKeyboardButton("🚨 Срочные заявки", callback_data="urgent_requests")],
            [InlineKeyboardButton("✅ Завершенные заявки", callback_data="completed_requests")],
            [InlineKeyboardButton("🛑 Остановить бота", callback_data="shutdown_bot")]
        ]
    
    # Agent menu
    elif role == SUPPORT_ROLES["agent"]:
        keyboard = [
            [InlineKeyboardButton("🔔 Активные заявки", callback_data="active_requests")],
            [InlineKeyboardButton("🚨 Срочные заявки", callback_data="urgent_requests")],
            [InlineKeyboardButton("✅ Завершенные заявки", callback_data="completed_requests")],
            [InlineKeyboardButton("ℹ️ Помощь", callback_data="help")]
        ]
    
    # Resident menu (checked by user_type)
    elif user_type == USER_TYPES["resident"]:
        keyboard = [
            [InlineKeyboardButton("📝 Новая заявка", callback_data="new_request")],
            [InlineKeyboardButton("📋 Мои заявки", callback_data="my_requests")],
            [InlineKeyboardButton("ℹ️ Помощь", callback_data="help")]
        ]
    
    # Potential buyer menu
    elif user_type == USER_TYPES["potential_buyer"]:
        keyboard = [
            [InlineKeyboardButton("ℹ️ О комплексе", callback_data="complex_info")],
            [InlineKeyboardButton("🏠 Цены на жилье", callback_data="pricing_info")],
            [InlineKeyboardButton("📞 Связаться с продажами", callback_data="sales_team")],
            [InlineKeyboardButton("❓ Задать вопрос", callback_data="ask_sales_question")]
        ]

    # Add back button if not in main menu and keyboard exists
    if not is_in_main_menu and keyboard:
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")])

    return InlineKeyboardMarkup(keyboard)

async def get_user_type(user_id: int) -> str:
    """Получает тип пользователя (resident или potential_buyer) из базы данных."""
    conn = None
    user_type = "unknown"
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT user_type FROM users WHERE user_id = %s", (user_id,))
            result = cur.fetchone()
            if result:
                user_type = result[0]
    except psycopg2.Error as e:
        logger.error(f"Database error in get_user_type for {user_id}: {e}")
    finally:
        if conn:
            conn.close()
    return user_type

def save_resident_to_db(user_id: int, data: dict):
    """Сохраняет нового резидента в таблицы users и residents."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Сначала добавляем или обновляем запись в таблице users
            # Устанавливаем роль 'resident' и тип 'resident'
            cur.execute(
                """
                INSERT INTO users (user_id, role, user_type) VALUES (%s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE SET role = EXCLUDED.role, user_type = EXCLUDED.user_type;
                """,
                (user_id, 'resident', 'resident')
            )
            
            # Затем добавляем детальную информацию в таблицу residents
            cur.execute(
                """
                INSERT INTO residents (user_id, full_name, address, phone_number) 
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE 
                SET full_name = EXCLUDED.full_name, address = EXCLUDED.address, phone_number = EXCLUDED.phone_number;
                """,
                (user_id, data['name'], data['address'], data['phone'])
            )
        conn.commit()
        logger.info(f"Successfully saved resident data for user {user_id}")
    except psycopg2.Error as e:
        logger.error(f"Database error in save_resident_to_db for {user_id}: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отправляет пользователю главное меню в зависимости от его роли."""
    message = update.message or update.callback_query.message
    chat_id = update.effective_user.id
    
    role = await get_user_role(chat_id)
    user_type = await get_user_type(chat_id)
    
    # Сохраняем актуальные данные в контекст
    context.user_data["role"] = role
    context.user_data["user_type"] = user_type

    text = "🏠 Главное меню:"
    
    await send_and_remember(
        update,
        context,
        text,
        main_menu_keyboard(chat_id, role, is_in_main_menu=True, user_type=user_type)
    )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the /start command and show appropriate menu."""
    chat_id = update.effective_user.id
    logger.info(f"User {chat_id} started bot.")

    # Полностью очищаем состояние пользователя при каждой команде /start
    context.user_data.clear()

    role = await get_user_role(chat_id)
    user_type = await get_user_type(chat_id) # Используем новую вспомогательную функцию
    context.user_data["user_type"] = user_type

    logger.info(f"User {chat_id} has role: {role} and user_type: {user_type}")

    # Генерируем соответствующее меню
    if role == SUPPORT_ROLES["agent"]:
        # Меню для агента не изменилось
        keyboard = [
            [InlineKeyboardButton("👷 Я сотрудник", callback_data="select_agent")],
            [InlineKeyboardButton("ℹ️ О комплексе", callback_data="complex_info")],
        ]
        await send_and_remember(
            update,
            context,
            "👷 Добро пожаловать! Вы зарегистрированы как сотрудник. Нажмите 'Я сотрудник', чтобы перейти в панель сотрудника.",
            InlineKeyboardMarkup(keyboard)
        )
    elif role == SUPPORT_ROLES["admin"]:
        # Меню для админа не изменилось
        await send_and_remember(
            update,
            context,
            "👑 Административное меню:",
            main_menu_keyboard(chat_id, role, is_in_main_menu=True, user_type=user_type)
        )
    elif user_type == USER_TYPES["resident"]:
         # Меню для резидента не изменилось
        await send_and_remember(
            update,
            context,
            "🏠 Добро пожаловать, резидент!",
            main_menu_keyboard(chat_id, role, is_in_main_menu=True, user_type=user_type)
        )
    else:
        # ИСПРАВЛЕНО: Меню для нового пользователя
        keyboard = [
            [InlineKeyboardButton("🏠 Я резидент (Регистрация)", callback_data="register_as_resident")],
            [InlineKeyboardButton("🛒 Я потенциальный покупатель", callback_data="select_potential_buyer")]
        ]
        await send_and_remember(
            update,
            context,
            "👋 Добро пожаловать в Sunqar Support Bot!\n\nПожалуйста, укажите, кто вы, чтобы продолжить:",
            InlineKeyboardMarkup(keyboard)
        )

async def register_as_resident(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle resident registration and ensure user_type and role are updated."""
    user_id = update.effective_user.id
    logger.info(f"User {user_id} initiated resident registration")

    # Clear previous state to avoid conflicts
    context.user_data.clear()

    # Set user_type to resident in context
    context.user_data["user_type"] = USER_TYPES["resident"]
    context.user_data["registration_flow"] = True
    context.user_data["awaiting_name"] = True

    # Update role and user_type in database
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (user_id, username, full_name, role, user_type, registration_date)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE 
                SET role = EXCLUDED.role, user_type = EXCLUDED.user_type, username = EXCLUDED.username, full_name = EXCLUDED.full_name
                """,
                (
                    user_id,
                    update.effective_user.username,
                    update.effective_user.full_name or "Unknown",
                    SUPPORT_ROLES["user"],  # Changed from SUPPORT_ROLES["resident"] to SUPPORT_ROLES["user"]
                    USER_TYPES["resident"],
                    datetime.now(timezone.utc)
                )
            )
            conn.commit()
            logger.info(f"Set user {user_id} as resident with user_type 'resident' in database")
    except psycopg2.Error as e:
        logger.error(f"Database error updating user {user_id}: {e}", exc_info=True)
        await send_and_remember(
            update,
            context,
            "❌ Ошибка базы данных. Попробуйте позже.",
            main_menu_keyboard(user_id, await get_user_role(user_id), user_type=USER_TYPES["resident"])
        )
        return
    finally:
        if conn:
            conn.close()

    await send_and_remember(
        update,
        context,
        "👤 Введите ваше ФИО для регистрации:",
        InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]])
    )

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

    # Clear stale user_data except user_type to prevent conflicts
    user_type = context.user_data.get("user_type")
    context.user_data.clear()
    if user_type:
        context.user_data["user_type"] = user_type

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
                    ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username, full_name = EXCLUDED.full_name
                    """,
                    (chat_id, username, full_name, SUPPORT_ROLES["user"], datetime.now()),
                )
                conn.commit()
                logger.info(f"Auto-registered user {chat_id} in users table")
    except psycopg2.Error as e:
        logger.error(f"Database error in process_new_request: {e}")
        conn.rollback()
        await send_and_remember(
            update,
            context,
            "❌ Ошибка базы данных. Попробуйте позже.",
            main_menu_keyboard(chat_id, await get_user_role(chat_id)),
        )
        return
    finally:
        conn.close()

    role = await get_user_role(chat_id)
    if role == SUPPORT_ROLES["admin"]:
        # For admins, skip resident check and prompt directly for problem description
        context.user_data["awaiting_problem"] = True
        await send_and_remember(
            update,
            context,
            "✍️ Опишите вашу проблему (для админа):",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]),
        )
    else:
        # For non-admins, proceed with resident check flow
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT resident_id FROM residents WHERE chat_id = %s", (chat_id,))
                resident = cur.fetchone()
                if resident:
                    # For registered residents, fetch details and prompt for problem
                    cur.execute(
                        "SELECT full_name, address, phone FROM residents WHERE chat_id = %s",
                        (chat_id,)
                    )
                    resident_data = cur.fetchone()
                    context.user_data["resident_id"] = resident[0]
                    context.user_data["user_name"] = resident_data[0]
                    context.user_data["user_address"] = resident_data[1]
                    context.user_data["user_phone"] = resident_data[2]
                    context.user_data["awaiting_problem"] = True
                    logger.info(f"Loaded resident data for chat_id {chat_id}: {context.user_data}")
                    await send_and_remember(
                        update,
                        context,
                        "✍️ Опишите вашу проблему:",
                        InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]),
                    )
                else:
                    # For non-registered residents, start registration flow
                    context.user_data["registration_flow"] = True
                    context.user_data["awaiting_name"] = True
                    logger.info(f"Starting registration flow for chat_id {chat_id}")
                    await send_and_remember(
                        update,
                        context,
                        "👤 Введите ваше ФИО:",
                        InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]),
                    )
        except psycopg2.Error as e:
            logger.error(f"Database error in resident check: {e}")
            await send_and_remember(
                update,
                context,
                "❌ Ошибка базы данных при проверке резидента. Попробуйте позже.",
                main_menu_keyboard(chat_id, role),
            )
            conn.rollback()
        finally:
            conn.close()
                
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
    """Process problem description and ensure user_type is updated to resident."""
    if not context.user_data.get("awaiting_problem"):
        logger.warning(f"User {update.effective_user.id} sent problem description outside expected flow")
        await send_and_remember(
            update,
            context,
            "❌ Ошибка: не ожидается ввод проблемы.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id), user_type=context.user_data.get("user_type"))
        )   
        return

    problem_text = update.message.text.strip()
    if not problem_text:
        logger.warning(f"User {update.effective_user.id} sent empty problem description")
        await send_and_remember(
            update,
            context,
            "❌ Описание проблемы не может быть пустым. Пожалуйста, опишите проблему:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]])
        )
        return

    # Store problem and determine urgency
    context.user_data["problem_text"] = problem_text
    urgent_keywords = ["потоп", "затоп", "пожар", "авария", "срочно", "опасно"]
    context.user_data["is_urgent"] = any(keyword in problem_text.lower() for keyword in urgent_keywords)
    context.user_data.pop("awaiting_problem", None)
    logger.info(f"Received problem: {problem_text} for chat_id: {update.effective_user.id}, is_urgent: {context.user_data['is_urgent']}")

    # Set user_type to resident since they're submitting a request
    context.user_data["user_type"] = USER_TYPES["resident"]

    # Validate required fields
    required_fields = ["user_name", "user_address", "user_phone", "problem_text"]
    missing_fields = [field for field in required_fields if field not in context.user_data or not context.user_data[field]]
    if missing_fields:
        logger.error(f"Missing fields in process_problem_report for user {update.effective_user.id}: {missing_fields}, user_data: {context.user_data}")
        await send_and_remember(
            update,
            context,
            f"❌ Ошибка: отсутствуют данные ({', '.join(missing_fields)}). Пожалуйста, начните процесс заново.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id), user_type=USER_TYPES["resident"])
        )
        return

    try:
        issue_id = await save_request_to_db(update, context, problem_text)
        if context.user_data["is_urgent"]:
            try:
                await send_urgent_alert(update, context, issue_id)
            except Exception as e:
                logger.error(f"Failed to send urgent alert for issue {issue_id}: {e}", exc_info=True)
        await send_and_remember(
            update,
            context,
            f"✅ Заявка принята!\n\n"
            f"{'🚨 Срочное обращение! Директор уведомлен.' if context.user_data['is_urgent'] else '⏳ Ожидайте ответа в течение 24 часов.'}\n"
            f"Номер заявки: #{issue_id}",
            main_menu_keyboard(update.effective_user.id, SUPPORT_ROLES["resident"], user_type=USER_TYPES["resident"])
        )
        # Update user_type in database
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE users 
                    SET user_type = %s 
                    WHERE user_id = %s
                    """,
                    (USER_TYPES["resident"], update.effective_user.id)
                )
                conn.commit()
                logger.info(f"Updated user_type to 'resident' for user {update.effective_user.id} in database")
        except psycopg2.Error as e:
            logger.error(f"Database error updating user_type for {update.effective_user.id}: {e}", exc_info=True)
        finally:
            if conn:
                conn.close()
        context.user_data.clear()
        context.user_data["user_type"] = USER_TYPES["resident"]
        logger.info(f"Cleared user_data and set user_type to resident for user {update.effective_user.id}")
    except ValueError as e:
        logger.error(f"Validation error in process_problem_report for user {update.effective_user.id}: {e}, user_data: {context.user_data}")
        await send_and_remember(
            update,
            context,
            f"❌ Ошибка: {e}. Пожалуйста, начните процесс заново.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id), user_type=USER_TYPES["resident"])
        )
    except psycopg2.Error as e:
        logger.error(f"Database error in process_problem_report for user {update.effective_user.id}: {e}", exc_info=True)
        await send_and_remember(
            update,
            context,
            f"❌ Ошибка базы данных при сохранении заявки: {e}. Попробуйте позже.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id), user_type=USER_TYPES["resident"])
        )
    except Exception as e:
        logger.error(f"Unexpected error in process_problem_report for user {update.effective_user.id}: {e}", exc_info=True)
        await send_and_remember(
            update,
            context,
            f"❌ Произошла ошибка при сохранении заявки: {e}. Попробуйте позже.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id), user_type=USER_TYPES["resident"])
        )

async def save_request_to_db(update: Update, context: ContextTypes.DEFAULT_TYPE, problem_text: str):
    chat_id = update.effective_user.id
    role = await get_user_role(chat_id)
    full_name = context.user_data.get("user_name", update.effective_user.full_name or "Unknown")
    address = context.user_data.get("user_address", "Админ" if role == SUPPORT_ROLES["admin"] else None)
    phone = context.user_data.get("user_phone", None)
    problem_text = context.user_data.get("problem_text", problem_text)
    urgent_keywords = ["потоп", "затоп", "пожар", "авария", "срочно", "опасно"]
    is_urgent = context.user_data.get("is_urgent", any(keyword in problem_text.lower() for keyword in urgent_keywords))
    logger.info(f"Saving request for user {chat_id}: user_data={context.user_data}, is_urgent={is_urgent}")

    # Validate required fields for non-admins
    if role != SUPPORT_ROLES["admin"]:
        required_fields = {
            "user_name": full_name,
            "user_address": address,
            "user_phone": phone,
            "problem_text": problem_text
        }
        missing_fields = [field for field, value in required_fields.items() if not value]
        if missing_fields:
            logger.error(f"Missing fields in save_request_to_db for user {chat_id}: {missing_fields}, user_data: {context.user_data}")
            raise ValueError(f"Отсутствуют данные: {', '.join(missing_fields)}")
        
        # Validate field types
        type_errors = []
        if not isinstance(full_name, str):
            type_errors.append("user_name должен быть строкой")
        if not isinstance(address, str):
            type_errors.append("user_address должен быть строкой")
        if not isinstance(phone, str):
            type_errors.append("user_phone должен быть строкой")
        if not isinstance(problem_text, str):
            type_errors.append("problem_text должен быть строкой")
        if type_errors:
            logger.error(f"Type errors in save_request_to_db for user {chat_id}: {type_errors}")
            raise ValueError(f"Ошибка в формате данных: {', '.join(type_errors)}")

    resident_id = None
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Register user in users table if missing
            cur.execute("SELECT 1 FROM users WHERE user_id = %s", (chat_id,))
            if not cur.fetchone():
                username = update.effective_user.username
                cur.execute(
                    """
                    INSERT INTO users (user_id, username, full_name, role, registration_date)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username, full_name = EXCLUDED.full_name
                    """,
                    (chat_id, username, full_name, SUPPORT_ROLES["user"], datetime.now()),
                )
                conn.commit()
                logger.info(f"Auto-registered user {chat_id} in users table")

            if role != SUPPORT_ROLES["admin"]:
                # Check if resident exists
                cur.execute(
                    "SELECT resident_id FROM residents WHERE chat_id = %s",
                    (chat_id,),
                )
                resident = cur.fetchone()
                if resident:
                    resident_id = resident[0]
                    logger.info(f"Found existing resident_id: {resident_id} for chat_id: {chat_id}")
                else:
                    # Create new resident
                    cur.execute(
                        """
                        INSERT INTO residents (chat_id, full_name, address, phone, registration_date)
                        VALUES (%s, %s, %s, %s, %s)
                        RETURNING resident_id
                        """,
                        (chat_id, full_name, address, phone, datetime.now()),
                    )
                    resident_id = cur.fetchone()[0]
                    conn.commit()
                    logger.info(f"Created new resident_id: {resident_id} for chat_id: {chat_id}")

            # Save the issue
            cur.execute(
                """
                INSERT INTO issues (resident_id, description, category, status, created_at)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING issue_id
                """,
                (
                    resident_id,
                    problem_text,
                    "urgent" if is_urgent else "normal",
                    "new",
                    datetime.now(),
                ),
            )
            issue_id = cur.fetchone()[0]
            conn.commit()
            logger.info(f"Saved issue #{issue_id} for chat_id: {chat_id}")

            # Log the issue creation
            cur.execute(
                """
                INSERT INTO issue_logs (issue_id, user_id, action, details, action_time)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    issue_id,
                    chat_id,
                    "created",
                    f"Новая заявка от {full_name}: {problem_text}",
                    datetime.now(),
                ),
            )
            conn.commit()
            logger.info(f"Logged issue creation for issue ID {issue_id}")

        return issue_id

    except psycopg2.Error as e:
        logger.error(f"Database error in save_request_to_db for user {chat_id}: {e}", exc_info=True)
        conn.rollback()
        raise
    except Exception as e:
        logger.error(f"Unexpected error in save_request_to_db for user {chat_id}: {e}", exc_info=True)
        conn.rollback()
        raise
    finally:
        if conn:
            logger.info("Closing database connection")
            conn.close()

from datetime import datetime, timezone, timedelta  # Add this import

async def send_urgent_alert(update: Update, context: ContextTypes.DEFAULT_TYPE, issue_id: int):
    """Send urgent alert to director."""
    try:
        user = update.effective_user
        full_name = context.user_data.get("user_name", user.full_name or "Unknown")
        phone = context.user_data.get("user_phone", "Не указан")
        address = context.user_data.get("user_address", "Не указан")
        problem_text = context.user_data.get("problem_text", "Не указана")
        timestamp = datetime.now(timezone(timedelta(hours=5))).strftime("%H:%M %d.%m.%Y")  # UTC+5

        message = (
            f"🚨 СРОЧНОЕ ОБРАЩЕНИЕ #{issue_id} 🚨\n\n"
            f"От: {full_name} (@{user.username or 'нет'})\n"
            f"ID: {user.id}\n"
            f"Адрес: {address}\n"
            f"Телефон: {phone}\n"
            f"Проблема: {problem_text}\n"
            f"Время: {timestamp}"
        )

        # Notify director
        await context.bot.send_message(
            chat_id=DIRECTOR_CHAT_ID,
            text=message,
        )
        logger.info(f"Sent urgent alert to director for issue #{issue_id}")

    except Exception as e:
        logger.error(f"Error sending urgent alert for issue #{issue_id}: {e}", exc_info=True)

async def process_user_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process user full name."""
    if not context.user_data.get("awaiting_name") or not context.user_data.get("registration_flow"):
        logger.warning(f"User {update.effective_user.id} sent name outside registration flow")
        await send_and_remember(
            update,
            context,
            "❌ Ошибка: не ожидается ввод ФИО.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
        return
    user_name = update.message.text.strip()
    if not user_name:
        logger.warning(f"User {update.effective_user.id} sent empty name")
        await send_and_remember(
            update,
            context,
            "❌ ФИО не может быть пустым. Пожалуйста, введите ваше ФИО:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]),
        )
        return
    context.user_data["user_name"] = user_name
    context.user_data["registration_flow"] = True
    context.user_data.pop("awaiting_name", None)
    context.user_data["awaiting_address"] = True
    logger.info(f"Stored user_name: {user_name} for chat_id: {update.effective_user.id}")
    await send_and_remember(
        update,
        context,
        "🏠 Введите ваш адрес (например: Корпус 1, кв. 25):",
        InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]),
    )

async def process_user_address(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process user address."""
    if not context.user_data.get("awaiting_address") or not context.user_data.get("registration_flow"):
        logger.warning(f"User {update.effective_user.id} sent address outside registration flow")
        await send_and_remember(
            update,
            context,
            "❌ Ошибка: не ожидается ввод адреса.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
        return
    user_address = update.message.text.strip()
    if not user_address:
        logger.warning(f"User {update.effective_user.id} sent empty address")
        await send_and_remember(
            update,
            context,
            "❌ Адрес не может быть пустым. Пожалуйста, введите ваш адрес:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]),
        )
        return
    context.user_data["user_address"] = user_address
    context.user_data["registration_flow"] = True
    context.user_data.pop("awaiting_address", None)
    context.user_data["awaiting_phone"] = True
    logger.info(f"Stored user_address: {user_address} for chat_id: {update.effective_user.id}")
    await send_and_remember(
        update,
        context,
        "📱 Введите ваш контактный телефон (например: +1234567890):",
        InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]),
    )

async def show_active_requests(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show active requests for agents with individual detail buttons."""
    if not await is_agent(update.effective_user.id):
        await update.callback_query.answer("❌ Доступ запрещен", show_alert=True)
        return
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT i.issue_id, r.full_name, i.description, i.created_at, i.category, r.address, r.phone
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

        for req in requests:
            issue_id, full_name, description, created_at, category, address, phone = req
            text = (
                f"🆔 Номер: #{issue_id}\n"
                f"👤 От: {full_name}\n"
                f"🏠 Адрес: {address}\n"
                f"📱 Телефон: {phone}\n"
                f"📅 Дата: {created_at.strftime('%d.%m.%Y %H:%M')}\n"
                f"🚨 Тип: {'Срочная' if category == 'urgent' else 'Обычная'}\n"
                f"📝 Описание: {description[:100]}{'...' if len(description) > 100 else ''}\n"
            )
            keyboard = [
                [InlineKeyboardButton("🔍 Подробности", callback_data=f"request_detail_{issue_id}")],
                [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
            ]
            await send_and_remember(
                update,
                context,
                text,
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
    """Show urgent requests for agents with individual detail buttons."""
    if not await is_agent(update.effective_user.id):
        await update.callback_query.answer("❌ Доступ запрещен", show_alert=True)
        return
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT i.issue_id, r.full_name, i.description, i.created_at, r.address, r.phone
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

        for req in requests:
            issue_id, full_name, description, created_at, address, phone = req
            text = (
                f"🆔 Номер: #{issue_id}\n"
                f"👤 От: {full_name}\n"
                f"🏠 Адрес: {address}\n"
                f"📱 Телефон: {phone}\n"
                f"📅 Дата: {created_at.strftime('%d.%m.%Y %H:%M')}\n"
                f"🚨 Тип: Срочная\n"
                f"📝 Описание: {description[:100]}{'...' if len(description) > 100 else ''}\n"
            )
            keyboard = [
                [InlineKeyboardButton("🔍 Подробности", callback_data=f"request_detail_{issue_id}")],
                [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
            ]
            await send_and_remember(
                update,
                context,
                text,
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
    if not query:
        logger.error("No callback query received")
        return
    await query.answer()
    user_id = update.effective_user.id
    role = await get_user_role(user_id)
    user_type = context.user_data.get("user_type", "unknown")
    logger.info(f"Processing button: {query.data} for user {user_id}")

    try:
        if query.data == "do_nothing":
            return
        elif query.data == "start":
            await start(update, context)
        elif query.data == "select_agent":
            if role == SUPPORT_ROLES["agent"]:
                await send_and_remember(
                    update,
                    context,
                    "👷 Панель сотрудника:",
                    main_menu_keyboard(user_id, role, is_in_main_menu=True)
                )
            else:
                await send_and_remember(
                    update,
                    context,
                    "❌ Вы не зарегистрированы как сотрудник.",
                    main_menu_keyboard(user_id, role)
                )
        elif query.data == "register_as_resident":
            context.user_data.clear()
            context.user_data["registration_flow"] = True
            context.user_data["awaiting_name"] = True
            logger.info(f"Starting registration flow for user {user_id}")
            await query.message.edit_text(
                "👤 Введите ваше ФИО:",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]])
            )
        elif query.data == "select_potential_buyer":
            await select_user_type(update, context, USER_TYPES["potential_buyer"])
        elif query.data == "complex_info":
            await show_complex_info(update, context)
        elif query.data == "pricing_info":
            await show_pricing_info(update, context)
        elif query.data == "sales_team":
            await show_sales_team(update, context)
        elif query.data == "ask_sales_question":
            if user_type != USER_TYPES["potential_buyer"]:
                await send_and_remember(
                    update,
                    context,
                    "❌ Только потенциальные покупатели могут задавать вопросы отделу продаж. Зарегистрируйтесь как потенциальный покупатель.",
                    main_menu_keyboard(user_id, role, user_type=user_type)
                )
                return
            context.user_data["awaiting_sales_question"] = True
            await send_and_remember(
                update,
                context,
                "❓ Пожалуйста, введите ваш вопрос для отдела продаж:",
                InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]])
            )
        elif query.data.startswith("reply_to_"):
            target_user_id = int(query.data.replace("reply_to_", ""))
            context.user_data["reply_to_user"] = target_user_id
            await send_and_remember(
                update,
                context,
                f"✍️ Введите ваш ответ для пользователя {target_user_id}:",
                InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]])
            )
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
                [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
            ]
            await send_and_remember(
                update,
                context,
                "📊 Выберите период отчета:",
                InlineKeyboardMarkup(keyboard)
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
        elif query.data.startswith("confirm_delete_"):
            resident_chat_id = int(query.data.split("_")[2])
            if "resident_to_delete" in context.user_data and context.user_data["resident_to_delete"]["chat_id"] == resident_chat_id:
                full_name = context.user_data["resident_to_delete"]["full_name"]
                resident_id = context.user_data["resident_to_delete"]["resident_id"]
                conn = None
                try:
                    conn = get_db_connection()
                    with conn.cursor() as cur:
                        cur.execute("SELECT COUNT(*) FROM issues WHERE resident_id = %s", (resident_id,))
                        issue_count = cur.fetchone()[0]
                        cur.execute("SELECT COUNT(*) FROM issue_logs WHERE issue_id IN (SELECT issue_id FROM issues WHERE resident_id = %s)", (resident_id,))
                        log_count = cur.fetchone()[0]
                        cur.execute("DELETE FROM residents WHERE chat_id = %s", (resident_chat_id,))
                        cur.execute("DELETE FROM users WHERE user_id = %s", (resident_chat_id,))
                        conn.commit()
                        await send_and_remember(
                            update,
                            context,
                            f"✅ Резидент {full_name} (chat ID: {resident_chat_id}) успешно удалён.\n"
                            f"Удалено заявок: {issue_count}, логов: {log_count}",
                            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id))
                        )
                        logger.info(f"Admin {update.effective_user.id} deleted resident {resident_chat_id} (resident_id: {resident_id})")
                except psycopg2.Error as e:
                    logger.error(f"Database error deleting resident {resident_chat_id}: {e}", exc_info=True)
                    if conn:
                        conn.rollback()
                    await send_and_remember(
                        update,
                        context,
                        f"❌ Ошибка базы данных при удалении резидента: {e}",
                        main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id))
                    )
                finally:
                    context.user_data.clear()
                    if conn:
                        conn.close()
            else:
                await send_and_remember(
                    update,
                    context,
                    "❌ Ошибка: данные для удаления не найдены.",
                    main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id))
                )
        elif query.data == "cancel":
            # Сохраняем важные данные перед очисткой
            saved_user_type = context.user_data.get("user_type")
            saved_role = role
            
            # Частичная очистка контекста
            context.user_data.clear()
            context.user_data["user_type"] = saved_user_type
            
            # Определяем текст в зависимости от роли
            if saved_role == SUPPORT_ROLES["admin"]:
                welcome_text = "👑 Административное меню:"
            elif saved_role == SUPPORT_ROLES["agent"]:
                welcome_text = "👷 Панель сотрудника:"
            elif saved_role == SUPPORT_ROLES["resident"]:
                welcome_text = "🏠 Главное меню:"
            else:
                welcome_text = "🏠Главное меню:" 
            
            await send_and_remember(
                update,
                context,
                welcome_text,
                main_menu_keyboard(user_id, saved_role, is_in_main_menu=True, user_type=saved_user_type)
            )
        elif query.data == "back_to_main":
            # Получаем актуальную роль на случай, если она изменилась
            current_role = await get_user_role(user_id)
            current_user_type = context.user_data.get("user_type", "unknown")
            
            # Определяем текст в зависимости от роли
            if current_role == SUPPORT_ROLES["admin"]:
                welcome_text = "👑 Административное меню:"
            elif current_role == SUPPORT_ROLES["agent"]:
                welcome_text = "👷 Панель сотрудника:"
            elif current_role == SUPPORT_ROLES["resident"]:
                welcome_text = "🏠 Главное меню:"
            else:
                welcome_text = "👋 Добро пожаловать! Пожалуйста, выберите действие:"
            
            await send_and_remember(
                update,
                context,
                welcome_text,
                main_menu_keyboard(user_id, current_role, is_in_main_menu=True, user_type=current_user_type)
            )
        else:
            logger.warning(f"Unknown command: {query.data}")
            await send_and_remember(
                update,
                context,
                "⚠️ Команда не распознана",
                main_menu_keyboard(user_id, role, user_type=user_type)
            )
    except psycopg2.Error as e:
        logger.error(f"Database error in button_handler for user {user_id}: {e}", exc_info=True)
        await send_and_remember(
            update,
            context,
            f"❌ Ошибка базы данных: {e}",
            main_menu_keyboard(user_id, role, user_type=user_type)
        )
    except Exception as e:
        logger.error(f"Unexpected error in button_handler for user {user_id}: {e}", exc_info=True)
        await send_and_remember(
            update,
            context,
            f"❌ Ошибка: {e}",
            main_menu_keyboard(user_id, role, user_type=user_type)
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
    if not re.match(r"^-?\d+$", agent_id_text):
        await send_and_remember(
            update,
            context,
            "❌ Неверный формат ID. Введите числовой Telegram ID (например, 123456789 или -123456789):",
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
        "📍 Расположение: г. Актобе\n"
        "🌳 Особенности: зеленые зоны, детские площадки, паркинг\n"
        "🏬 Типы квартир: 1, 2, 3-комнатные\n"
        "📞 Контакт: @ShiriOni99"
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
    """Handle the submission of a sales question from a potential buyer."""
    if "awaiting_sales_question" not in context.user_data:
        return  # Ignore if not waiting for a question

    question = update.message.text.strip()
    user_id = update.effective_user.id
    username = update.effective_user.username or "No username"
    full_name = update.effective_user.full_name or "Unknown"
    timestamp = datetime.now().strftime("%H:%M %d.%m.%Y")  # Format: 07:54 30.06.2025

    # Query all agents (role = 2)
    conn = None
    agents = []
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT user_id FROM users WHERE role = %s", (SUPPORT_ROLES["agent"],))
            agents = [row[0] for row in cur.fetchall()]
    except psycopg2.Error as e:
        logger.error(f"Database error getting agents: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

    # Include director if defined
    recipients = agents + ([int(DIRECTOR_CHAT_ID)] if DIRECTOR_CHAT_ID else [])

    # Format notification message
    notification_text = (
        f"❓ Новый вопрос от потенциального покупателя:\n"
        f"👤 От: {full_name} (@{username})\n"
        f"🆔 ID: {user_id}\n"
        f"📝 Вопрос: {question}\n"
        f"🕒 Время: {timestamp}"
    )

    # Send notification to all agents and director
    failed_recipients = []
    for recipient_id in recipients:
        try:
            await context.bot.send_message(
                chat_id=recipient_id,
                text=notification_text,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📞 Ответить", callback_data=f"reply_to_{user_id}")]
                ])
            )
            logger.info(f"Sent sales question to recipient {recipient_id}")
        except (telegram.error.BadRequest, telegram.error.Forbidden) as e:
            logger.warning(f"Failed to send sales question to {recipient_id}: {e}")
            failed_recipients.append(recipient_id)

    # Notify user their question was sent
    await send_and_remember(
        update,
        context,
        "✅ Ваш вопрос отправлен в отдел продаж. Ожидайте ответа!",
        main_menu_keyboard(user_id, await get_user_role(user_id), is_in_main_menu=True, user_type=context.user_data.get("user_type")),
    )

    # Notify director about failed recipients (if any)
    if failed_recipients and DIRECTOR_CHAT_ID:
        try:
            await context.bot.send_message(
                chat_id=DIRECTOR_CHAT_ID,
                text=f"⚠️ Не удалось отправить вопрос следующим сотрудникам: {', '.join(map(str, failed_recipients))}. "
                     f"Убедитесь, что они запустили бота с /start."
            )
        except telegram.error.TelegramError:
            logger.error(f"Failed to notify director about failed recipients")

    # Clear state
    context.user_data.pop("awaiting_sales_question", None)

async def process_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle replies from agents/directors to users."""
    if "reply_to_user" not in context.user_data:
        return  # Ignore if not waiting for a reply

    reply_text = update.message.text.strip()
    target_user_id = context.user_data["reply_to_user"]
    sender_id = update.effective_user.id
    sender_role = await get_user_role(sender_id)

    if sender_role not in [SUPPORT_ROLES["agent"], SUPPORT_ROLES["admin"]]:
        await send_and_remember(
            update,
            context,
            "❌ Только сотрудники могут отправлять ответы.",
            main_menu_keyboard(sender_id, sender_role, user_type=context.user_data.get("user_type"))
        )
        return

    try:
        await context.bot.send_message(
            chat_id=target_user_id,
            text=f"📬 Ответ от отдела продаж:\n{reply_text}"
        )
        await send_and_remember(
            update,
            context,
            f"✅ Ответ отправлен пользователю {target_user_id}.",
            main_menu_keyboard(sender_id, sender_role, is_in_main_menu=True, user_type=context.user_data.get("user_type"))
        )
    except (telegram.error.BadRequest, telegram.error.Forbidden) as e:
        logger.error(f"Failed to send reply to {target_user_id}: {e}")
        await send_and_remember(
            update,
            context,
            f"❌ Не удалось отправить ответ: пользователь {target_user_id} не запустил бота.",
            main_menu_keyboard(sender_id, sender_role, is_in_main_menu=True, user_type=context.user_data.get("user_type"))
        )

    context.user_data.pop("reply_to_user", None)

async def delete_resident(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_user.id
    role = await get_user_role(chat_id)
    if role != SUPPORT_ROLES["admin"]:
        await update.callback_query.answer("❌ Только администраторы могут удалять резидентов.", show_alert=True)
        return

    # Clear any conflicting states to avoid routing to wrong handlers
    context.user_data.clear()
    context.user_data["awaiting_resident_id_delete"] = True
    logger.info(f"User {chat_id} initiated resident deletion, set state: awaiting_resident_id_delete")

    await send_and_remember(
        update,
        context,
        "🗑 Введите chat ID резидента для удаления:",
        InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]])
    )

async def process_resident_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаление резидента с улучшенной обработкой ошибок и каскадным удалением."""
    if "awaiting_resident_id_delete" not in context.user_data:
        logger.warning(f"No awaiting_resident_id_delete state for user {update.effective_user.id}")
        await send_and_remember(
            update,
            context,
            "❌ Ошибка: не ожидается ввод chat ID для удаления.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id))
        )
        return

    chat_id_input = update.message.text.strip()
    logger.info(f"Received chat_id input for deletion: '{chat_id_input}' from user {update.effective_user.id}")

    try:
        resident_chat_id = int(chat_id_input)  # Define resident_chat_id here
    except ValueError:
        logger.error(f"Invalid chat_id format: '{chat_id_input}'")
        await send_and_remember(
            update,
            context,
            "❌ Неверный формат chat ID. Введите числовой ID (например, 123456789).",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]])
        )
        return

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Check if resident exists
            cur.execute("SELECT resident_id, full_name FROM residents WHERE chat_id = %s", (resident_chat_id,))
            resident = cur.fetchone()
            if not resident:
                logger.info(f"No resident found with chat_id {resident_chat_id}")
                await send_and_remember(
                    update,
                    context,
                    f"❌ Резидент с chat ID {resident_chat_id} не найден.",
                    main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id))
                )
                return

            resident_id, full_name = resident
            # Count related issues and logs for logging
            cur.execute("SELECT COUNT(*) FROM issues WHERE resident_id = %s", (resident_id,))
            issue_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM issue_logs WHERE issue_id IN (SELECT issue_id FROM issues WHERE resident_id = %s)", (resident_id,))
            log_count = cur.fetchone()[0]

            # Delete resident (cascades to issues and issue_logs)
            cur.execute("DELETE FROM residents WHERE chat_id = %s", (resident_chat_id,))
            # Delete user from users table
            cur.execute("DELETE FROM users WHERE user_id = %s", (resident_chat_id,))
            conn.commit()

            logger.info(f"Admin {update.effective_user.id} deleted resident {resident_chat_id} (resident_id: {resident_id}) with {issue_count} issues and {log_count} logs")
            await send_and_remember(
                update,
                context,
                f"✅ Резидент {full_name} (chat ID: {resident_chat_id}) успешно удалён.\n"
                f"Удалено заявок: {issue_count}, логов: {log_count}",
                main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id))
            )
    except psycopg2.Error as e:
        logger.error(f"Database error deleting resident {resident_chat_id}: {e}", exc_info=True)
        if conn:
            conn.rollback()
        await send_and_remember(
            update,
            context,
            f"❌ Ошибка базы данных при удалении резидента: {e}",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id))
        )
    except Exception as e:
        logger.error(f"Unexpected error deleting resident {resident_chat_id}: {e}", exc_info=True)
        await send_and_remember(
            update,
            context,
            f"❌ Непредвиденная ошибка: {e}",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id))
        )
    finally:
        context.user_data.clear()  # Clear all states after completion
        if conn:
            conn.close()
        
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
                INSERT INTO users (user_id, username, full_name, role, user_type, registration_date)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE 
                SET username = EXCLUDED.username, 
                    full_name = EXCLUDED.full_name, 
                    role = EXCLUDED.role, 
                    user_type = EXCLUDED.user_type,
                    registration_date = EXCLUDED.registration_date
                """,
                (chat_id, username, full_name, SUPPORT_ROLES["resident"], USER_TYPES["resident"], datetime.now()),
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

# ... (previous code, including process_new_resident_phone)

# Эти функции нужно вставить ПЕРЕД save_user_data

async def handle_name_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Saves user's name and asks for address."""
    context.user_data['name'] = update.message.text
    context.user_data['state'] = 'awaiting_address'
    await update.message.reply_text("Отлично! Теперь введите ваш адрес (например, Улица Абая 1, кв 1):")

async def handle_address_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Saves user's address and asks for phone."""
    context.user_data['address'] = update.message.text
    context.user_data['state'] = 'awaiting_phone'
    await update.message.reply_text("Принято. Теперь введите ваш номер телефона (например, +7 777 123 4567):")

async def handle_phone_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Saves phone, completes registration, and shows the main menu."""
    user_id = update.effective_user.id
    context.user_data['phone'] = update.message.text
    
    # Сохраняем все данные в базу данных
    save_resident_to_db(user_id, context.user_data)
    
    await update.message.reply_text("✅ Спасибо! Ваша регистрация в качестве резидента успешно завершена.")
    
    # Очищаем состояние регистрации
    for key in ['state', 'name', 'address', 'phone']:
        if key in context.user_data:
            del context.user_data[key]
            
    # Вызываем главное меню для нового резидента
    await main_menu(update, context)

# Update save_user_data to route to handle_name_input
# Это твоя обновленная функция save_user_data

async def save_user_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Routes user input based on the current state."""
    user_id = update.effective_user.id
    state = context.user_data.get('state') # Получаем текущее состояние
    
    logger.info(f"User {user_id} in state '{state}' sent text: {update.message.text}")

    # Маршрутизация на основе одного состояния
    if state == 'awaiting_name':
        await handle_name_input(update, context)
    elif state == 'awaiting_address':
        await handle_address_input(update, context)
    elif state == 'awaiting_phone':
        await handle_phone_input(update, context) # <-- ГЛАВНОЕ ИСПРАВЛЕНИЕ ЗДЕСЬ
    
    # Остальные состояния, которые у тебя были
    elif state == "awaiting_problem":
        await process_problem_report(update, context)
    elif state == "awaiting_solution":
        await save_solution(update, context)
    elif state == "awaiting_agent_id":
        await process_new_agent(update, context)
    elif state == "awaiting_agent_name":
        await save_agent(update, context)
    elif state == "awaiting_resident_id_delete":
        await process_resident_delete(update, context)
    elif state == "awaiting_sales_question":
        await process_sales_question(update, context)
    # ... и так далее для всех остальных состояний ...
    
    else:
        logger.warning(f"No awaiting state for user {user_id} or state is None.")
        # Если состояние не определено, показываем главное меню
        await main_menu(update, context)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors."""
    error = context.error
    logger.error("Exception occurred:", exc_info=error)
    
    if isinstance(error, (NetworkError, TimedOut)):
        logger.warning(f"⚠️ Network error occurred: {error}. Attempting to reconnect...")
        if update and update.effective_user:
            await send_and_remember(
                update,
                context,
                "⚠️ Проблема с сетью. Пожалуйста, попробуйте позже.",
                main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id))
            )
        return
    
    if isinstance(error, KeyError) and "resident" in str(error):
        logger.error(f"KeyError: 'resident' not found in SUPPORT_ROLES, user_id: {update.effective_user.id if update else 'unknown'}")
        if update and update.effective_user:
            await send_and_remember(
                update,
                context,
                "❌ Ошибка: роль 'resident' не определена. Пожалуйста, свяжитесь с администратором.",
                InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="start")]])
            )
        return
    
    if update and update.effective_user:
        await send_and_remember(
            update,
            context,
            "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже или обратитесь в техподдержку.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id))
        )

import threading
from http.server import HTTPServer

# Global variable to hold the server instance
health_server = None

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            try:
                with get_db_connection() as conn:
                    self.wfile.write(b'OK DB OK')
            except Exception as e:
                self.wfile.write(f'DB ERROR: {str(e)}'.encode())
        else:
            self.send_response(404)
            self.end_headers()

def run_health_check():
    global health_server
    port = int(os.getenv("PORT", 8080))
    health_server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
    logger.info(f"✅ Health check server running on port {port} (PID: {os.getpid()})")
    health_server.serve_forever()

def start_health_server():
    global health_server
    server_thread = threading.Thread(target=run_health_check, daemon=True)
    server_thread.start()
    time.sleep(5)
    return server_thread

def stop_health_server():
    global health_server
    if health_server:
        health_server.shutdown()
        health_server.server_close()
        logger.info("Health check server stopped")

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

            # Add handlers
            application.add_handler(CommandHandler("start", start))
            application.add_handler(CommandHandler("report", generate_report_command))
            application.add_handler(CommandHandler("clear", clear_chat))
            application.add_handler(CallbackQueryHandler(button_handler))
            application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, save_user_data, block=False))
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