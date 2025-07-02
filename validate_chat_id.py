# Импортируем нужные библиотеки
import re  # Для очистки текста
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes
import logging

# Настраиваем логирование, чтобы видеть ошибки
logger = logging.getLogger(__name__)

async def validate_chat_id(chat_id_input: str, update: Update = None, context: ContextTypes.DEFAULT_TYPE = None) -> int:
    """Проверяет, что введенный chat_id — это правильное число.
    
    Args:
        chat_id_input: Текст, который нужно проверить (например, '123' или 'abc').
        update: Объект Telegram (опционально, для отправки сообщений об ошибке).
        context: Объект Telegram (опционально, для отправки сообщений).
    
    Returns:
        int: Правильный chat_id (число).
    
    Raises:
        ValueError: Если chat_id неправильный. Если переданы update и context, отправляется сообщение об ошибке.
    """
    try:
        # Удаляем всё, кроме цифр и минуса (например, '123abc' -> '123')
        cleaned_input = re.sub(r'[^\d-]', '', chat_id_input)
        # Превращаем текст в число
        chat_id = int(cleaned_input)
        # Проверяем, что число не равно 0 и не слишком большое (Telegram использует 64-битные числа)
        if chat_id == 0 or abs(chat_id) > 2**63-1:
            raise ValueError("Chat ID вне допустимого диапазона")
        return chat_id
    except ValueError:
        # Пишем в лог, что произошла ошибка
        logger.error(f"Неправильный формат chat_id: '{chat_id_input}'")
        # Если переданы update и context, отправляем сообщение пользователю
        if update and context:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="❌ Неверный формат chat ID. Введите корректный числовой ID.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Отмена", callback_data="cancel")]
                ])
            )
        raise ValueError("Неправильный chat_id")

import re

def validate_director_chat_id(chat_id_input: str) -> int:
    if not chat_id_input:
        raise ValueError("DIRECTOR_CHAT_ID environment variable is missing")
    try:
        cleaned_input = re.sub(r'[^\d-]', '', chat_id_input)
        chat_id = int(cleaned_input)
        if chat_id == 0 or abs(chat_id) > 2**63-1:
            raise ValueError("Chat ID outside valid range")
        return chat_id
    except ValueError as e:
        raise ValueError(f"Invalid DIRECTOR_CHAT_ID format: {str(e)}")