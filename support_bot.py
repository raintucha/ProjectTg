import logging 
import os
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

# Load configuration
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
DIRECTOR_CHAT_ID = os.getenv("DIRECTOR_CHAT_ID")
NEWS_CHANNEL = os.getenv("NEWS_CHANNEL", "@sunqar_news")
DATABASE_URL = os.getenv("DATABASE_URL")

# Validate environment variables
if not TELEGRAM_TOKEN or not DIRECTOR_CHAT_ID or not DATABASE_URL:
    raise ValueError("Missing required environment variables: TELEGRAM_TOKEN and DIRECTOR_CHAT_ID and DATABASE_URL")

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
    if "last_message_id" in context.user_data:
        try:
            await context.bot.delete_message(
                chat_id=update.effective_chat.id,
                message_id=context.user_data["last_message_id"],
            )
            logger.info(f"Deleted previous message ID {context.user_data['last_message_id']}")
        except Exception as e:
            logger.warning(f"Failed to delete message: {e}")
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
    """Deprecated: Use send_message_with_keyboard instead."""
    return await send_message_with_keyboard(update, context, text, reply_markup)


async def safe_db_connection(retries=3, delay=2):
    """Try to establish DB connection with retries."""
    import asyncio
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
        # Optionally notify user or ignore silently

import asyncio

async def clear_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /clear command to fully reset chat history as fast as possible."""
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    
    try:
        # Clear all context data
        context.user_data.clear()
        context.chat_data.clear()
        
        # Get the latest message ID
        current_message_id = update.message.message_id
        
        # Collect message IDs to delete (assuming we try up to 1000 messages)
        message_ids = list(range(max(1, current_message_id - 1000), current_message_id + 1))
        
        # Function to attempt deleting a single message
        async def delete_single_message(msg_id):
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
            except Exception:
                pass  # Silently skip errors (e.g., message not found or no permissions)
        
        # Delete messages in parallel batches
        batch_size = 50  # Telegram rate limits suggest batches of ~50
        for i in range(0, len(message_ids), batch_size):
            batch = message_ids[i:i + batch_size]
            await asyncio.gather(*[delete_single_message(msg_id) for msg_id in batch])
        
        # Send message prompting to restart
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
    # Use a cleaner shutdown if possible
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
    # Validate phone format
    phone = update.message.text.strip()
    phone_pattern = re.compile(r"^\+?\d{7,15}$")
    if not phone_pattern.match(phone):
        await send_and_remember(
            update,
            context,
            "❌ Неверный формат телефона. Пожалуйста, введите корректный номер (например, +71234567890):",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]),
        )
        return

    # Sanitize phone number
    phone = re.sub(r"[^\d+]", "", phone)
    
    conn = None
    try:
        # Validate all required fields exist
        required_fields = {
            'user_name': str,
            'user_address': str,
            'problem_text': str,
            'is_urgent': bool
        }
        
        missing_fields = [field for field in required_fields if field not in context.user_data]
        if missing_fields:
            logger.error(f"Missing required fields: {missing_fields}")
            await send_and_remember(
                update,
                context,
                "❌ Ошибка: отсутствуют необходимые данные. Пожалуйста, начните процесс заново.",
                main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
            )
            return

        # Validate field types
        type_errors = []
        for field, field_type in required_fields.items():
            if not isinstance(context.user_data[field], field_type):
                type_errors.append(f"{field} должен быть {field_type.__name__}")
        
        if type_errors:
            logger.error(f"Type errors: {type_errors}")
            await send_and_remember(
                update,
                context,
                "❌ Ошибка в формате данных. Пожалуйста, начните процесс заново.",
                main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
            )
            return

        conn = get_db_connection()
        with conn.cursor() as cur:
            # Check if user already exists
            cur.execute(
                "SELECT resident_id FROM residents WHERE chat_id = %s",
                (update.effective_user.id,)
            )
            if cur.fetchone():
                await send_and_remember(
                    update,
                    context,
                    "ℹ️ Вы уже зарегистрированы. Создаем новую заявку.",
                    main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
                )

            # Save resident data
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

            # Save issue
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

            # Log the action
            cur.execute(
                """INSERT INTO issue_logs (issue_id, action, user_id, action_time)
                VALUES (%s, 'create', %s, NOW())""",
                (issue_id, update.effective_user.id)
            )
            
            conn.commit()

            # Send urgent alert if needed
            if context.user_data["is_urgent"]:
                await send_urgent_alert(update, context, issue_id)

            # Success message
            await send_and_remember(
                update,
                context,
                "✅ Регистрация и заявка приняты!\n\n"
                f"{'🚨 Срочное обращение! Директор уведомлен.' if context.user_data['is_urgent'] else '⏳ Ожидайте ответа в течение 24 часов.'}\n"
                f"Номер заявки: #{issue_id}",
                main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
            )
            
            # Clear context
            context.user_data.clear()

    except psycopg2.IntegrityError as e:
        logger.error(f"Database integrity error: {e}")
        await send_and_remember(
            update,
            context,
            "❌ Ошибка: проблема с сохранением данных. Пожалуйста, попробуйте позже.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
        if conn:
            conn.rollback()
            
    except psycopg2.Error as e:
        logger.error(f"Database error during registration: {e}")
        await send_and_remember(
            update,
            context,
            "❌ Ошибка базы данных. Пожалуйста, попробуйте позже.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
        if conn:
            conn.rollback()
            
    except Exception as e:
        logger.error(f"Unexpected error during registration: {e}", exc_info=True)
        await send_and_remember(
            update,
            context,
            "❌ Непредвиденная ошибка. Пожалуйста, попробуйте позже.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
        if conn:
            conn.rollback()
            
    finally:
        if conn:
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
        await safe_send_message

def main_menu_keyboard(user_id, role):
    """Generate main menu keyboard based on user role."""
    keyboard = []
    if role == SUPPORT_ROLES["user"]:
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
        keyboard.append([InlineKeyboardButton("📬 Активные заявки", callback_data="active_requests")])
        keyboard.append([InlineKeyboardButton("🚨 Срочные заявки", callback_data="urgent_requests")])
        keyboard.append([InlineKeyboardButton("📖 Завершенные заявки", callback_data="completed_requests")])
        keyboard.append([InlineKeyboardButton("🛑 Завершить работу бота", callback_data="shutdown_bot")])
    keyboard.append([InlineKeyboardButton("🔙 Главное меню", callback_data="start")])
    return InlineKeyboardMarkup(keyboard)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start command."""
    user_id = update.effective_user.id
    role = await get_user_role(user_id)
    await send_and_remember(
        update,
        context,
        "🏠 Добро пожаловать в службу поддержки ЖК!",
        main_menu_keyboard(user_id, role),
    )

async def process_new_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Initiate new request process."""
    await send_and_remember(
        update,
        context,
        "✍️ Опишите вашу проблему:",
        InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]),
    )
    context.user_data["awaiting_problem"] = True

async def show_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display help information."""
    await send_and_remember(
        update,
        context,
        f"ℹ️ Справка:\n\n• Для срочных проблем используйте слова: 'потоп', 'пожар', 'авария'\n"
        f"• Новости ЖК: {NEWS_CHANNEL}\n• Техподдержка: @ShiroOni99",
        main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
    )

async def show_user_requests(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user's recent requests."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
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
        logger.error(f"Error retrieving user requests: {e}")
        await send_and_remember(
            update,
            context,
            "❌ Ошибка при получении данных.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
    finally:
        if conn:
            conn.close()

async def process_problem_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process problem description."""
    problem_text = update.message.text
    context.user_data["problem_text"] = problem_text
    urgent_keywords = ["потоп", "затоп", "пожар", "авария", "срочно", "опасно"]
    is_urgent = any(keyword in problem_text.lower() for keyword in urgent_keywords)
    context.user_data["is_urgent"] = is_urgent
    
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Проверяем, зарегистрирован ли пользователь
            cur.execute(
                "SELECT resident_id FROM residents WHERE chat_id = %s",
                (update.effective_user.id,)
            )
            resident = cur.fetchone()

        if resident:
            # Если пользователь зарегистрирован - сохраняем заявку
            issue_id = await save_request_to_db(update, context, resident[0])
            await send_and_remember(
                update,
                context,
                f"✅ Ваша заявка сохранена!\nНомер заявки: #{issue_id}\nНажмите '🔙 Главное меню' для продолжения.",
                main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
            )
            if context.user_data["is_urgent"]:
                await send_urgent_alert(update, context, issue_id)
            return

        # Если пользователь не зарегистрирован - начинаем процесс регистрации
        await send_and_remember(
            update,
            context,
            "📝 Для регистрации введите ваше ФИО:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]])
        )
        context.user_data.pop("awaiting_problem", None)
        context.user_data["awaiting_name"] = True
        
    except psycopg2.Error as e:
        logger.error(f"Database error in process_problem_report: {e}")
        await send_and_remember(
            update,
            context,
            "❌ Ошибка базы данных. Пожалуйста, попробуйте позже.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id))
        )
    finally:
        if conn:
            conn.close()
            
async def save_request_to_db(update: Update, context: ContextTypes.DEFAULT_TYPE, resident_id: int):
    logger.info(f"Attempting to save request. Resident ID: {resident_id}")
    logger.info(f"Context data: {context.user_data}")
    
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Проверка существования resident_id
            cur.execute("SELECT 1 FROM residents WHERE resident_id = %s", (resident_id,))
            if not cur.fetchone():
                logger.error(f"Resident {resident_id} not found in database")
                raise ValueError(f"Resident {resident_id} not found")
            
            # Проверка данных
            required_fields = ['problem_text', 'is_urgent']
            for field in required_fields:
                if field not in context.user_data:
                    logger.error(f"Missing required field: {field}")
                    raise ValueError(f"Missing {field}")
            
            # Сохранение заявки
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
            conn.commit()
            
            logger.info(f"Successfully saved issue #{issue_id}")
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

import re

async def process_user_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process user phone and save resident data with validation."""
    phone = update.message.text.strip()
    phone_pattern = re.compile(r"^\+?\d{7,15}$")  # Simple phone validation: digits with optional +, length 7-15
    if not phone_pattern.match(phone):
        await send_and_remember(
            update,
            context,
            "❌ Неверный формат телефона. Пожалуйста, введите корректный номер (например, +71234567890):",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]),
        )
        return

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO residents (chat_id, full_name, address, phone, registration_date)
                VALUES (%s, %s, %s, %s, %s) RETURNING resident_id
            """,
                (
                    update.effective_user.id,
                    context.user_data["user_name"],
                    context.user_data["user_address"],
                    phone,
                    datetime.now(),
                ),
            )
            resident_id = cur.fetchone()[0]
            cur.execute(
                """
                INSERT INTO issues (resident_id, description, category, status, created_at)
                VALUES (%s, %s, %s, 'new', %s) RETURNING issue_id
            """,
                (
                    resident_id,
                    context.user_data["problem_text"],
                    "urgent" if context.user_data["is_urgent"] else "normal",
                    datetime.now(),
                ),
            )
            issue_id = cur.fetchone()[0]
            conn.commit()

        if context.user_data["is_urgent"]:
            await send_urgent_alert(update, context, issue_id)

        await send_and_remember(
            update,
            context,
            "✅ Регистрация и заявка приняты!\n\n"
            f"{'🚨 Срочное обращение! Директор уведомлен.' if context.user_data['is_urgent'] else '⏳ Ожидайте ответа в течение 24 часов.'}",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
        context.user_data.clear()
    except psycopg2.Error as e:
        logger.error(f"Error during registration: {e}")
        await send_and_remember(
            update,
            context,
            "❌ Ошибка при регистрации.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
    finally:
        if conn:
            conn.close()

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

        # Добавляем только кнопку "Назад"
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

def generate_pdf_report(start_date, end_date):
    """Generate PDF report."""
    conn = None
    try:
        pdf = FPDF()
        pdf.add_page()
        
        # Load DejaVuSans font for Cyrillic support
        font_path = "DejaVuSans.ttf"
        if not os.path.exists(font_path):
            logger.error(f"Font file {font_path} not found. Please place it in the script directory.")
            raise Exception(f"Font file {font_path} not found. Please download DejaVuSans.ttf.")
        
        pdf.add_font("DejaVuSans", "", font_path, uni=True)
        pdf.set_font("DejaVuSans", "", 12)

        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT r.full_name, r.address, i.description, 
                       i.category, i.status, i.created_at, i.completed_at, 
                       COALESCE(u.full_name, 'Не указан') as closed_by
                FROM issues i
                JOIN residents r ON i.resident_id = r.resident_id
                LEFT JOIN users u ON i.closed_by = u.user_id
                WHERE i.created_at BETWEEN %s AND %s
                ORDER BY i.created_at DESC
            """,
                (start_date, end_date),
            )
            issues = cur.fetchall()

        # Helper function to clean text for PDF
        def clean_text(text):
            try:
                return text.encode('utf-8', errors='replace').decode('utf-8')
            except Exception as e:
                logger.error(f"Error cleaning text '{text}': {e}")
                return str(text).encode('ascii', errors='replace').decode('ascii')

        # Set title
        pdf.cell(200, 10, txt=clean_text("Отчет по заявкам ЖК"), ln=1, align="C")
        pdf.cell(
            200,
            10,
            txt=clean_text(f"Период: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}"),
            ln=1,
            align="C",
        )
        pdf.ln(10)

        # Calculate dynamic column widths based on content
        col_widths = [40, 40, 60, 25, 25, 30]  # Initial widths
        for issue in issues:
            col_widths[0] = max(col_widths[0], len(clean_text(issue[0])) * 2.5)
            col_widths[1] = max(col_widths[1], len(clean_text(issue[1])) * 2.5)
            col_widths[2] = max(col_widths[2], len(clean_text(issue[2])) * 2.5)
            col_widths[3] = max(col_widths[3], len(clean_text("Сроч" if issue[3] == "urgent" else "Обыч")) * 2.5)
            col_widths[4] = max(col_widths[4], len(clean_text(issue[4])) * 2.5)
            col_widths[5] = max(col_widths[5], len(clean_text(issue[7])) * 2.5)

        # Ensure total width doesn't exceed page width (adjust if necessary)
        total_width = sum(col_widths)
        if total_width > 190:  # Page width is ~200mm, leave some margin
            scale_factor = 190 / total_width
            col_widths = [w * scale_factor for w in col_widths]

        # Set table headers
        pdf.set_font("DejaVuSans", size=10)
        headers = ["ФИО", "Адрес", "Описание", "Тип", "Статус", "Закрыл"]
        for i, header in enumerate(headers):
            pdf.cell(col_widths[i], 8, clean_text(header), border=1, align="C")
        pdf.ln()

        # Add table rows with MultiCell for text wrapping
        base_height = 5  # Base height per line
        for issue in issues:
            full_name = clean_text(issue[0])
            address = clean_text(issue[1])
            description = clean_text(issue[2])
            category = clean_text("Сроч" if issue[3] == "urgent" else "Обыч")
            status = clean_text(issue[4])
            closed_by = clean_text(issue[7])

            # Calculate number of lines for each cell (approximate)
            def get_line_count(text, width):
                return max(1, int(len(text) * pdf.font_size / (width / 2.5)))

            max_lines = max(
                get_line_count(full_name, col_widths[0]),
                get_line_count(address, col_widths[1]),
                get_line_count(description, col_widths[2]),
                get_line_count(category, col_widths[3]),
                get_line_count(status, col_widths[4]),
                get_line_count(closed_by, col_widths[5])
            )
            row_height = base_height * max_lines

            # Starting x position for the row
            start_x = pdf.get_x()
            start_y = pdf.get_y()

            # Use MultiCell for each column
            pdf.multi_cell(col_widths[0], base_height, full_name, border=1, align="L")
            pdf.set_xy(start_x + col_widths[0], start_y)
            pdf.multi_cell(col_widths[1], base_height, address, border=1, align="L")
            pdf.set_xy(start_x + col_widths[0] + col_widths[1], start_y)
            pdf.multi_cell(col_widths[2], base_height, description, border=1, align="L")
            pdf.set_xy(start_x + col_widths[0] + col_widths[1] + col_widths[2], start_y)
            pdf.multi_cell(col_widths[3], base_height, category, border=1, align="C")
            pdf.set_xy(start_x + col_widths[0] + col_widths[1] + col_widths[2] + col_widths[3], start_y)
            pdf.multi_cell(col_widths[4], base_height, status, border=1, align="C")
            pdf.set_xy(start_x + col_widths[0] + col_widths[1] + col_widths[2] + col_widths[3] + col_widths[4], start_y)
            pdf.multi_cell(col_widths[5], base_height, closed_by, border=1, align="L")

            # Move to the next row
            pdf.set_xy(start_x, start_y + row_height)

        # Save PDF
        os.makedirs("reports", exist_ok=True)
        filename = f"reports/report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        pdf.output(filename)
        logger.info(f"PDF report generated: {filename}")
        return filename
    except psycopg2.Error as e:
        logger.error(f"Database error generating PDF: {e}")
        raise Exception(f"Database error: {e}")
    except Exception as e:
        logger.error(f"Error generating PDF: {e}")
        raise
    finally:
        if conn:
            conn.close()

async def generate_report_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Generate report command."""
    if not await is_admin(update.effective_user.id):
        await update.callback_query.answer("❌ Эта команда доступна только администратору.", show_alert=True)
        return
    keyboard = [
        [InlineKeyboardButton("📅 За последние 7 дней", callback_data="report_7")],
        [InlineKeyboardButton("📅 За последние 30 дней", callback_data="report_30")],
        [InlineKeyboardButton("📅 За текущий месяц", callback_data="report_month")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel")],
    ]
    await send_and_remember(
        update,
        context,
        "📊 Выберите период для отчета:",
        InlineKeyboardMarkup(keyboard),
    )

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
    await generate_and_send_report(update, context, start_date, end_date)

async def generate_and_send_report(
    update: Update, context: ContextTypes.DEFAULT_TYPE, start_date: datetime, end_date: datetime
):
    """Generate and send PDF report."""
    processing_msg = await update.effective_chat.send_message("🔄 Генерация отчета...")
    try:
        report_path = generate_pdf_report(start_date, end_date)
        with open(report_path, "rb") as report_file:
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=report_file,
                caption=f"📊 Отчет за период с {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')}",
            )
        await processing_msg.delete()
        await start(update, context)
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        await processing_msg.edit_text(f"❌ Ошибка генерации отчета: {e}")

async def manage_agents_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show agent management menu."""
    if not await is_admin(update.effective_user.id):
        await update.callback_query.answer("❌ Доступ запрещен", show_alert=True)
        return
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT user_id, username, full_name, role 
                FROM users 
                WHERE role IN (%s, %s)
                ORDER BY role DESC
            """,
                (SUPPORT_ROLES["agent"], SUPPORT_ROLES["admin"]),
            )
            agents = cur.fetchall()

        keyboard = [
            [
                InlineKeyboardButton(
                    f"{'👑 Админ' if agent[3] == SUPPORT_ROLES['admin'] else '🛠️ Агент'}: {agent[2]} (@{agent[1] or 'нет'})",
                    callback_data=f"agent_info_{agent[0]}",
                )
            ]
            for agent in agents
        ]
        keyboard.append([InlineKeyboardButton("➕ Добавить агента", callback_data="add_agent")])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")])

        text = "👥 Управление персоналом:\n\n"
        if not agents:
            text += "Нет зарегистрированных сотрудников"

        await send_and_remember(
            update,
            context,
            text,
            InlineKeyboardMarkup(keyboard),
        )
    except psycopg2.Error as e:
        logger.error(f"Database error in manage_agents_menu: {e}")
        await send_and_remember(
            update,
            context,
            f"❌ Ошибка базы данных: {e}",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
    finally:
        if conn:
            conn.close()

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
        elif query.data == "new_request":
            await process_new_request(update, context)
        elif query.data == "my_requests":
            await show_user_requests(update, context)
        elif query.data == "help":
            await show_help(update, context)
        elif query.data == "active_requests":
            await show_active_requests(update, context)
        elif query.data == "urgent_requests":
            await show_urgent_requests(update, context)
        elif query.data == "completed_requests":
            await completed_requests(update, context)
        elif query.data == "reports_menu":
            await generate_report_command(update, context)
        elif query.data == "manage_agents":
            await manage_agents_menu(update, context)
        elif query.data == "shutdown_bot":
            await shutdown_bot(update, context)
        elif query.data == "confirm_shutdown":
            await send_and_remember(update, context, "🛑 Бот останавливается...")
            os._exit(0)
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
                main_menu_keyboard(user_id, role),
            )
    except psycopg2.Error as e:
        logger.error(f"Database error in button_handler: {e}")
        await send_and_remember(
            update,
            context,
            f"❌ Ошибка базы данных: {e}",
            main_menu_keyboard(user_id, role),
        )
    except Exception as e:
        logger.error(f"Unexpected error in button_handler: {e}")
        await send_and_remember(
            update,
            context,
            f"❌ Ошибка: {e}",
            main_menu_keyboard(user_id, role),
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

import re

async def process_new_agent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process new agent ID with validation."""
    agent_id_text = update.message.text.strip()
    if not re.match(r"^\d{5,20}$", agent_id_text):  # Telegram IDs are numeric, length 5-20 digits
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

async def save_agent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Save new agent to database."""
    if (
        "new_agent_id" not in context.user_data
        or "awaiting_agent_name" not in context.user_data
    ):
        await send_and_remember(
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
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM users WHERE user_id = %s", (agent_id,))
            if cur.fetchone():
                await send_and_remember(
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
        await send_and_remember(
            update,
            context,
            f"✅ Новый агент {agent_name} (ID: {agent_id}) успешно добавлен!",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
        context.user_data.pop("new_agent_id", None)
        context.user_data.pop("awaiting_agent_name", None)
    except psycopg2.Error as e:
        logger.error(f"Error adding agent: {e}")
        await send_and_remember(
            update,
            context,
            "❌ Ошибка при добавлении агента.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
    finally:
        if conn:
            conn.close()

async def save_user_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text messages based on context."""
    if "awaiting_problem" in context.user_data:
        await process_problem_report(update, context)
    elif "awaiting_name" in context.user_data:
        await process_user_name(update, context)
    elif "awaiting_address" in context.user_data:
        await process_user_address(update, context)
    elif "awaiting_phone" in context.user_data:
        await process_user_phone(update, context)
    elif "awaiting_solution" in context.user_data:
        await save_solution(update, context)
    elif "awaiting_agent_id" in context.user_data:
        await process_new_agent(update, context)
    elif "awaiting_agent_name" in context.user_data:
        await save_agent(update, context)
    elif "awaiting_user_message" in context.user_data:
        await send_user_message(update, context)

from telegram.error import NetworkError, TimedOut

from telegram.error import NetworkError, TimedOut

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors."""
    error = context.error
    if isinstance(error, (NetworkError, TimedOut)):
        print(f"⚠️ Network error: {error}. Reconnecting...")
        return
    
    logger.error("Exception:", exc_info=error)
    if update and update.effective_user:
        await send_and_remember(
            update,
            context,
            "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.",
            main_menu_keyboard(update.effective_user.id, await get_user_role(update.effective_user.id)),
        )
        
import os
from threading import Thread
from http.server import BaseHTTPRequestHandler, HTTPServer
import time
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackQueryHandler

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            # Проверяем соединения с БД и Telegram
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
    port = int(os.getenv("PORT", 10000))
    with HTTPServer(('0.0.0.0', port), HealthCheckHandler) as server:
        print(f"✅ Health check server running on port {port} (PID: {os.getpid()})")
        server.serve_forever()

def start_health_server():
    server_thread = Thread(target=run_health_check, daemon=True)
    server_thread.start()
    # Ждем чтобы сервер точно запустился
    time.sleep(5)
    return server_thread

def main() -> None:
    """Run the bot with auto-restart."""
    while True:
        try:
            health_server = start_health_check()
            print("🔄 Initializing bot...")
            application = Application.builder().token(TELEGRAM_TOKEN).build()

            application.add_handler(CommandHandler("start", start))
            application.add_handler(CommandHandler("report", generate_report_command))
            application.add_handler(CommandHandler("clear", clear_chat))
            application.add_handler(CallbackQueryHandler(button_handler))
            application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, save_user_data))
            application.add_error_handler(error_handler)

            print("🚀 Starting bot polling...")
            application.run_polling(
                drop_pending_updates=True,
                close_loop=False,  # Важно для переподключения
                allowed_updates=Update.ALL_TYPES
            )
        except Exception as e:
            print(f"⚠️ Bot crashed: {e}")
            print("🔄 Restarting in 10 seconds...")
            time.sleep(10)

if __name__ == '__main__':
    print("🛠 Starting application...")
    # Дополнительная задержка для Render
    time.sleep(8)
    main()