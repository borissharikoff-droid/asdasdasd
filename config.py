import os

# Конфигурация бота — значения берутся из переменных окружения (если заданы)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8361266417:AAEfwm_4kJHnLopUyH_sA3nArNcb42CcRpQ")
GOOGLE_SHEETS_ID = os.getenv("GOOGLE_SHEETS_ID", "1KGi1sDNqFzSZwDJLa9zcCXAv6fwbyOmEF-34eZdQKXc")

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
