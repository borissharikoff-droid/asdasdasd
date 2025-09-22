import os

def _require_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(f"Environment variable {var_name} is required but not set")
    return value

# Конфигурация бота — значения берутся из переменных окружения (обязательные)
TELEGRAM_BOT_TOKEN = _require_env("TELEGRAM_BOT_TOKEN")
GOOGLE_SHEETS_ID = _require_env("GOOGLE_SHEETS_ID")

# Пути к файлам
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE", "credentials.json")
CREDENTIALS_FOLDER = os.getenv("CREDENTIALS_FOLDER", "credentials")

# Настройки Google Sheets
SHEET_SCOPE = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# Заголовки таблицы
SHEET_HEADERS = ['Покупатель', 'Дата', 'Время', 'Сумма', 'Валюта', 'Тип оплаты', 'Формат', 'Внешняя/Внутренняя', 'Канал где была публикация', 'Комментарий']

# Настройки логирования
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# ID чата для пересылки уведомлений о продажах
NOTIFICATION_CHAT_ID = os.getenv("NOTIFICATION_CHAT_ID", "")
