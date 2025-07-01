import asyncio
import logging
import os
from enum import Enum, auto
from datetime import datetime

import psycopg2
from dotenv import load_dotenv
from fpdf import FPDF
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (Application, CallbackQueryHandler, CommandHandler,
                          ContextTypes, MessageHandler, filters)

# --- 1. Конфигурация и Константы ---
load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
DIRECTOR_ID = os.getenv("DIRECTOR_ID")
# Убедись, что файл шрифта DejaVuSans.ttf лежит рядом с ботом
FONT_PATH = "DejaVuSans.ttf"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

class Role(Enum):
    """Роли пользователей для четкого контроля доступа."""
    NEW_USER = auto()
    RESIDENT = auto()
    POTENTIAL_BUYER = auto()
    AGENT = auto()
    ADMIN = auto()

class State(Enum):
    """Состояния диалога для управления логикой."""
    REGISTER_RESIDENT_NAME = auto()
    REGISTER_RESIDENT_ADDRESS = auto()
    REGISTER_RESIDENT_PHONE = auto()
    AWAITING_PROBLEM_DESCRIPTION = auto()
    AWAITING_SOLUTION_TEXT = auto()
    AWAITING_SALES_QUESTION = auto()


# --- 2. Работа с Базой Данных ---

def get_db_connection():
    """Возвращает синхронное соединение с БД."""
    return psycopg2.connect(DATABASE_URL)

def db_init_tables():
    """Создает все необходимые таблицы, если их нет."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    role VARCHAR(50) NOT NULL DEFAULT 'new_user',
                    user_type VARCHAR(50),
                    registration_date TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS residents (
                    user_id BIGINT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
                    full_name VARCHAR(255),
                    address VARCHAR(255),
                    phone_number VARCHAR(50)
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS issues (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT REFERENCES users(user_id),
                    description TEXT NOT NULL,
                    is_urgent BOOLEAN DEFAULT FALSE,
                    status VARCHAR(50) DEFAULT 'new',
                    solution TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    resolved_at TIMESTAMP WITH TIME ZONE,
                    resolved_by_user_id BIGINT
                );
            """)
        conn.commit()
        logger.info("Database tables initialized successfully.")
    finally:
        conn.close()

def db_get_user_data(user_id: int) -> dict:
    """Получает роль и тип пользователя."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT role, user_type FROM users WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
            if row:
                return {"role": row[0], "user_type": row[1]}
            return {"role": "new_user", "user_type": None}
    finally:
        conn.close()

def db_register_user(user_id: int, role: str, user_type: str, details: dict = None):
    """Регистрирует или обновляет пользователя."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (user_id, role, user_type) VALUES (%s, %s, %s) "
                "ON CONFLICT (user_id) DO UPDATE SET role = EXCLUDED.role, user_type = EXCLUDED.user_type",
                (user_id, role, user_type)
            )
            if user_type == 'resident' and details:
                cur.execute(
                    "INSERT INTO residents (user_id, full_name, address, phone_number) VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT (user_id) DO UPDATE SET full_name = EXCLUDED.full_name, address = EXCLUDED.address, phone_number = EXCLUDED.phone_number",
                    (user_id, details['name'], details['address'], details['phone'])
                )
        conn.commit()
    finally:
        conn.close()

def db_create_issue(user_id: int, description: str, is_urgent: bool) -> int:
    """Создает новую заявку и возвращает ее ID."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO issues (user_id, description, is_urgent, status) VALUES (%s, %s, %s, 'new') RETURNING id",
                (user_id, description, is_urgent)
            )
            issue_id = cur.fetchone()[0]
            conn.commit()
            return issue_id
    finally:
        conn.close()

def db_get_issues(user_id: int = None, status: str = None) -> list:
    """Получает заявки из БД с возможностью фильтрации."""
    conn = get_db_connection()
    query = "SELECT id, user_id, description, status, created_at FROM issues"
    filters, params = [], []

    if user_id:
        filters.append("user_id = %s")
        params.append(user_id)
    if status:
        filters.append("status = %s")
        params.append(status)

    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += " ORDER BY created_at DESC"

    try:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            issues = [dict(zip([desc[0] for desc in cur.description], row)) for row in cur.fetchall()]
            return issues
    finally:
        conn.close()

def db_complete_issue(issue_id: int, solution: str, admin_id: int):
    """Отмечает заявку как завершенную."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE issues SET status = 'completed', solution = %s, resolved_by_user_id = %s, resolved_at = NOW() WHERE id = %s",
                (solution, admin_id, issue_id)
            )
        conn.commit()
        logger.info(f"Issue {issue_id} completed by user {admin_id}.")
    finally:
        conn.close()


# --- 3. Генерация Клавиатур ---

async def get_keyboard_for_user(user_id: int) -> InlineKeyboardMarkup:
    """Создает и возвращает клавиатуру на основе роли пользователя."""
    user_data = await asyncio.to_thread(db_get_user_data, user_id)
    role_str = user_data.get("role")
    role_map = {"resident": Role.RESIDENT, "potential_buyer": Role.POTENTIAL_BUYER, "agent": Role.AGENT, "admin": Role.ADMIN}
    role = role_map.get(role_str, Role.NEW_USER)

    keyboard = []
    if role == Role.ADMIN:
        keyboard.append([InlineKeyboardButton("👑 Панель Администратора", callback_data="admin_panel")])
    
    if role == Role.AGENT or role == Role.ADMIN:
        keyboard.extend([
            [InlineKeyboardButton("👀 Новые заявки", callback_data="agent_view_new")],
            [InlineKeyboardButton("✅ Завершенные заявки", callback_data="agent_view_completed")]
        ])

    if role == Role.RESIDENT:
        keyboard.extend([
            [InlineKeyboardButton("📝 Создать заявку", callback_data="create_issue")],
            [InlineKeyboardButton("📂 Мои заявки", callback_data="my_issues")]
        ])

    if role == Role.POTENTIAL_BUYER:
        keyboard.extend([
            [InlineKeyboardButton("ℹ️ О комплексе", callback_data="complex_info")],
            [InlineKeyboardButton("🏠 Цены на жилье", callback_data="pricing_info")],
            [InlineKeyboardButton("❓ Задать вопрос", callback_data="ask_sales_question")]
        ])

    if role == Role.NEW_USER:
        keyboard = [[InlineKeyboardButton("🏠 Я резидент (Регистрация)", callback_data="register_resident")],
                    [InlineKeyboardButton("🛒 Я покупатель", callback_data="register_buyer")]]

    return InlineKeyboardMarkup(keyboard)

def get_admin_panel_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 Сгенерировать PDF отчет", callback_data="admin_pdf_report")],
        [InlineKeyboardButton("⬅️ Назад в главное меню", callback_data="main_menu")]
    ])


# --- 4. Обработчики Команд, Кнопок и Сообщений ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    context.user_data.clear()
    keyboard = await get_keyboard_for_user(user_id)
    await update.message.reply_text("👋 Добро пожаловать! Выберите действие:", reply_markup=keyboard)

async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    context.user_data.clear()
    keyboard = await get_keyboard_for_user(user_id)
    message = update.callback_query.message if update.callback_query else update.message
    await message.reply_text("🏠 Главное меню:", reply_markup=keyboard)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    parts = query.data.split(':')
    action, value = parts[0], parts[1] if len(parts) > 1 else None

    # Роутинг
    if action == "main_menu": await main_menu(update, context)
    elif action == "register_resident":
        context.user_data['state'] = State.REGISTER_RESIDENT_NAME
        await query.edit_message_text("Введите ваше полное имя (ФИО):")
    elif action == "register_buyer":
        await asyncio.to_thread(db_register_user, user_id, "potential_buyer", "potential_buyer")
        keyboard = await get_keyboard_for_user(user_id)
        await query.edit_message_text("Вы зарегистрированы как потенциальный покупатель!", reply_markup=keyboard)
    elif action == "create_issue":
        context.user_data['state'] = State.AWAITING_PROBLEM_DESCRIPTION
        await query.edit_message_text("Пожалуйста, подробно опишите вашу проблему:")
    elif action == "my_issues":
        issues = await asyncio.to_thread(db_get_issues, user_id=user_id)
        if not issues:
            await query.edit_message_text("У вас пока нет заявок.", reply_markup=await get_keyboard_for_user(user_id))
        else:
            response = "Ваши заявки:\n\n"
            for issue in issues:
                response += f"• ID: {issue['id']}, Статус: {issue['status']}\n  `{issue['description'][:50]}...`\n\n"
            await query.edit_message_text(response, parse_mode='Markdown')
    elif action == "agent_view_new":
        issues = await asyncio.to_thread(db_get_issues, status='new')
        if not issues:
            await query.edit_message_text("Нет новых заявок.", reply_markup=await get_keyboard_for_user(user_id))
        else:
            await query.edit_message_text("Новые заявки:")
            for issue in issues:
                keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Завершить эту заявку", callback_data=f"complete_issue:{issue['id']}")]])
                await query.message.reply_text(f"ID: {issue['id']}\nОписание: {issue['description']}", reply_markup=keyboard)
    elif action == "complete_issue" and value:
        context.user_data['state'] = State.AWAITING_SOLUTION_TEXT
        context.user_data['issue_to_complete'] = value
        await query.message.reply_text(f"Введите текст решения для заявки №{value}:")
    elif action == "admin_panel":
        await query.edit_message_text("👑 Панель администратора:", reply_markup=get_admin_panel_keyboard())
    elif action == "admin_pdf_report":
        all_issues = await asyncio.to_thread(db_get_issues)
        # Здесь логика создания PDF
        pdf = FPDF()
        pdf.add_page()
        pdf.add_font('DejaVu', '', FONT_PATH, uni=True)
        pdf.set_font('DejaVu', '', 14)
        pdf.cell(200, 10, txt="Отчет по заявкам", ln=True, align='C')
        for issue in all_issues:
            pdf.set_font('DejaVu', '', 10)
            pdf.multi_cell(0, 5, txt=f"ID: {issue['id']}, Статус: {issue['status']}\nОписание: {issue['description']}\n\n")
        report_filename = "report.pdf"
        pdf.output(report_filename)
        await query.message.reply_document(document=open(report_filename, 'rb'), filename="Отчет по заявкам.pdf")

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = context.user_data.get('state')
    if not state:
        await update.message.reply_text("Неизвестная команда, используйте /start или кнопки.")
        return

    user_id = update.effective_user.id
    text = update.message.text
    
    if state == State.REGISTER_RESIDENT_NAME:
        context.user_data['details'] = {'name': text}
        context.user_data['state'] = State.REGISTER_RESIDENT_ADDRESS
        await update.message.reply_text("Введите ваш адрес:")
    elif state == State.REGISTER_RESIDENT_ADDRESS:
        context.user_data['details']['address'] = text
        context.user_data['state'] = State.REGISTER_RESIDENT_PHONE
        await update.message.reply_text("Введите ваш номер телефона:")
    elif state == State.REGISTER_RESIDENT_PHONE:
        context.user_data['details']['phone'] = text
        await asyncio.to_thread(db_register_user, user_id, "resident", "resident", context.user_data['details'])
        context.user_data.clear()
        await update.message.reply_text("✅ Регистрация завершена!")
        await main_menu(update, context)
    elif state == State.AWAITING_PROBLEM_DESCRIPTION:
        is_urgent = any(word in text.lower() for word in ["срочно", "потоп", "авария"])
        issue_id = await asyncio.to_thread(db_create_issue, user_id, text, is_urgent)
        context.user_data.clear()
        await update.message.reply_text(f"✅ Ваша заявка №{issue_id} принята!")
        if is_urgent and DIRECTOR_ID:
            await context.bot.send_message(DIRECTOR_ID, f"‼️ СРОЧНАЯ ЗАЯВКА №{issue_id} от пользователя {user_id}:\n\n{text}")
        await main_menu(update, context)
    elif state == State.AWAITING_SOLUTION_TEXT:
        issue_id = context.user_data.get('issue_to_complete')
        await asyncio.to_thread(db_complete_issue, int(issue_id), text, user_id)
        context.user_data.clear()
        await update.message.reply_text(f"✅ Заявка №{issue_id} успешно завершена!")
        await main_menu(update, context)

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await main_menu(update.callback_query if update.callback_query else update, context)

# --- 5. Запуск Бота ---

def main():
    # Инициализируем таблицы при первом запуске, если нужно
    # db_init_tables()
    
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(cancel, pattern="^cancel$"))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    
    logger.info("Bot is starting...")
    application.run_polling()

if __name__ == "__main__":
    main()