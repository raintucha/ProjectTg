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

def validate_director_chat_id(director_chat_id: str) -> int:
    """Проверяет DIRECTOR_CHAT_ID при запуске бота.
    
    Args:
        director_chat_id: Значение DIRECTOR_CHAT_ID из переменной окружения.
    
    Returns:
        int: Правильный chat_id.
    
    Raises:
        ValueError: Если DIRECTOR_CHAT_ID некорректен.
    """
    try:
        return validate_chat_id(director_chat_id)
    except ValueError:
        logger.error(f"Некорректный DIRECTOR_CHAT_ID: '{director_chat_id}'")
        raise ValueError("DIRECTOR_CHAT_ID должен быть числом")