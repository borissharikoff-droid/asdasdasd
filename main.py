import os
import re
import logging
import signal
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import telebot
from telebot import types
import gspread
from google.oauth2.service_account import Credentials

import config

# Настройка логирования
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT
)
logger = logging.getLogger(__name__)

class SalesBot:
    def __init__(self):
        self.bot_token = config.TELEGRAM_BOT_TOKEN
        if not self.bot_token:
            raise ValueError("TELEGRAM_BOT_TOKEN не найден в конфигурации")
        
        self.bot = telebot.TeleBot(self.bot_token)
        # На всякий случай убираем вебхук перед polling, чтобы избежать 409 Conflict
        try:
            self.bot.remove_webhook()
        except Exception as e:
            logger.warning(f"Не удалось снять webhook: {e}")
        self.sheets_id = config.GOOGLE_SHEETS_ID
        self.sheet = None
        self.stats = {
            'total_usdt': 0,
            'total_rub': 0,
            'total_sales': 0,
            'sales_by_payment': {}
        }
        
        # Настройка Google Sheets
        self._setup_google_sheets()
        
        # Регистрация обработчиков
        self._register_handlers()
        
    def _setup_google_sheets(self):
        """Настройка подключения к Google Sheets"""
        # Проверяем, отключены ли Google Sheets
        if os.getenv('DISABLE_GOOGLE_SHEETS', '').lower() in ['true', '1', 'yes']:
            logger.info("Google Sheets отключен через переменную окружения DISABLE_GOOGLE_SHEETS")
            self.sheet = None
            return
            
        try:
            import json
            creds_data = None

            # 1) Пробуем читать креды из переменной окружения (удобно для Railway)
            creds_env = os.getenv('GOOGLE_CREDENTIALS_JSON')
            if creds_env:
                try:
                    creds_data = json.loads(creds_env)
                except json.JSONDecodeError:
                    # Возможно, экранированные \n мешают. Попробуем исправить и распарсить снова
                    try:
                        fixed = creds_env.replace('\\n', '\n')
                        creds_data = json.loads(fixed)
                    except Exception as e:
                        raise ValueError(f"GOOGLE_CREDENTIALS_JSON некорректен: {e}")
            else:
                # 2) Иначе ищем файл на диске
                creds_file = config.CREDENTIALS_FILE
                if not os.path.exists(creds_file):
                    creds_file = os.path.join(config.CREDENTIALS_FOLDER, 'credentials.json')

                if not os.path.exists(creds_file):
                    logger.info("Файл с кредами Google API не найден - работаем без Google Sheets")
                    self.sheet = None
                    return

                logger.info(f"Используется файл с кредами: {creds_file}")
                try:
                    with open(creds_file, 'r', encoding='utf-8') as f:
                        creds_data = json.load(f)
                except json.JSONDecodeError as e:
                    logger.warning(f"Файл credentials.json содержит некорректный JSON: {e}")
                    self.sheet = None
                    return

            # Валидация и нормализация ключа
            required_fields = ['type', 'project_id', 'private_key', 'client_email']
            missing_fields = [field for field in required_fields if field not in creds_data]
            if missing_fields:
                logger.warning(f"В credentials отсутствуют обязательные поля: {missing_fields}")
                self.sheet = None
                return
            if creds_data.get('type') != 'service_account':
                logger.warning("Требуется сервисный аккаунт (type: 'service_account')")
                self.sheet = None
                return

            # Нормализуем переносы строк в private_key (часто проблема из-за \n)
            if isinstance(creds_data.get('private_key'), str):
                creds_data['private_key'] = creds_data['private_key'].replace('\\n', '\n')

            creds = Credentials.from_service_account_info(creds_data, scopes=config.SHEET_SCOPE)
            gc = gspread.authorize(creds)
            
            # Открываем таблицу
            self.sheet = gc.open_by_key(self.sheets_id).sheet1
            
            # Создаем заголовки если их нет или если они неправильные
            first_row = self.sheet.get('A1:G1')
            if not first_row or not first_row[0] or first_row[0][0] != 'Покупатель':
                # Очищаем первую строку если нужно
                if first_row and first_row[0]:
                    self.sheet.delete_rows(1)
                # Добавляем правильные заголовки
                self.sheet.insert_row(config.SHEET_HEADERS, 1)
                
            logger.info("Google Sheets подключен успешно")
            
        except Exception as e:
            logger.warning(f"Не удалось подключиться к Google Sheets: {e}")
            logger.warning("Бот будет работать в режиме симуляции - данные не будут записываться в таблицу")
            self.sheet = None
    
    def _register_handlers(self):
        """Регистрация обработчиков команд"""
        
        @self.bot.message_handler(commands=['start'])
        def start_command(message):
            self._handle_start(message)
        
        @self.bot.message_handler(commands=['stats'])
        def stats_command(message):
            self._handle_stats(message)
        
        @self.bot.message_handler(func=lambda message: True)
        def handle_message(message):
            self._handle_sales_message(message)
    
    def _handle_start(self, message):
        """Обработчик команды /start"""
        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(types.InlineKeyboardButton(
            "📊 Открыть таблицу", 
            url=f"https://docs.google.com/spreadsheets/d/{self.sheets_id}"
        ))
        
        welcome_text = """
🤖 <b>Бот для учета продаж рекламы</b>

<b>Как использовать:</b>
Отправьте сообщение в формате:
• <code>@менеджер дата время сумма [формат] канал</code>

<b>Примеры:</b>
• <code>@maxim 12 декабря 11:11 1489usdt 1/24 BusinessChannel</code>
• <code>@anna 14.05 11:11 500р каналбизнес</code>

<b>Доступные команды:</b>
/start - Главное меню
/stats - Статистика продаж

<b>Поддерживаемые валюты:</b>
• USDT (usdt, $)
• Рубли (р, руб, ₽)

<b>Доступные форматы:</b>
• 1/24
• 1/48
        """
        
        self.bot.send_message(
            message.chat.id, 
            welcome_text, 
            parse_mode='HTML',
            reply_markup=keyboard
        )
    
    def _handle_stats(self, message):
        """Обработчик команды /stats"""
        stats_text = f"""
📊 <b>Статистика продаж</b>

💰 <b>Общая сумма:</b>
• USDT: {self.stats['total_usdt']:.2f}
• Рубли: {self.stats['total_rub']:.2f} ₽

📈 <b>Количество продаж:</b> {self.stats['total_sales']}

💳 <b>По методам оплаты:</b>
"""
        
        for payment_method, count in self.stats['sales_by_payment'].items():
            stats_text += f"• {payment_method}: {count}\n"
        
        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(types.InlineKeyboardButton(
            "📊 Открыть таблицу", 
            url=f"https://docs.google.com/spreadsheets/d/{self.sheets_id}"
        ))
        
        self.bot.send_message(
            message.chat.id, 
            stats_text, 
            parse_mode='HTML',
            reply_markup=keyboard
        )
    
    def _parse_sales_message(self, text: str) -> Optional[Dict]:
        """Парсинг сообщения о продаже"""
        # Паттерны для различных форматов
        patterns = [
            # @maxim 12 декабря 11:11 1489usdt 1/24 BusinessChannel (с форматом в канале)
            r'@(\w+)\s+(\d{1,2}\s+\w+)\s+(\d{1,2}:\d{2})\s+(\d+(?:\.\d+)?)(usdt|р|руб|\$|₽|юсдт)\s+(\d+/\d+)\s+(.+)',
            # @maxim 12.12 11:11 1489usdt 1/24 BusinessChannel (с форматом в канале)
            r'@(\w+)\s+(\d{1,2}\.\d{1,2})\s+(\d{1,2}:\d{2})\s+(\d+(?:\.\d+)?)(usdt|р|руб|\$|₽|юсдт)\s+(\d+/\d+)\s+(.+)',
            # Ксения Вантрип 1230 16.04 501юсдт 1/24 БиБ (новый формат без @)
            r'(\w+\s+\w+)\s+(\d{1,2}\d{2})\s+(\d{1,2}\.\d{1,2})\s+(\d+(?:\.\d+)?)(usdt|р|руб|\$|₽|юсдт)\s+(\d+/\d+)\s+(.+)',
            # Ксения Вантрип 12:30 16.04 501юсдт 1/24 БиБ (с двоеточием)
            r'(\w+\s+\w+)\s+(\d{1,2}:\d{2})\s+(\d{1,2}\.\d{1,2})\s+(\d+(?:\.\d+)?)(usdt|р|руб|\$|₽|юсдт)\s+(\d+/\d+)\s+(.+)',
            # @похуй 12.04 1719 522р 1/24 "АНУС" (с форматом)
            r'@(\w+)\s+(\d{1,2}\.\d{1,2})\s+(\d{1,2}\d{2})\s+(\d+(?:\.\d+)?)(usdt|р|руб|\$|₽|юсдт)\s+(\d+/\d+)\s+(.+)',
            # @похуй 12.04 17:19 522р 1/24 "АНУС" (с форматом и двоеточием)
            r'@(\w+)\s+(\d{1,2}\.\d{1,2})\s+(\d{1,2}:\d{2})\s+(\d+(?:\.\d+)?)(usdt|р|руб|\$|₽|юсдт)\s+(\d+/\d+)\s+(.+)',
            # @maxim 12 декабря 11:11 1489usdt BusinessChannel (без формата)
            r'@(\w+)\s+(\d{1,2}\s+\w+)\s+(\d{1,2}:\d{2})\s+(\d+(?:\.\d+)?)(usdt|р|руб|\$|₽|юсдт)\s+(.+)',
            # @maxim 14.05 11:11 500р каналбизнес (без формата)
            r'@(\w+)\s+(\d{1,2}\.\d{1,2})\s+(\d{1,2}:\d{2})\s+(\d+(?:\.\d+)?)(usdt|р|руб|\$|₽|юсдт)\s+(.+)',
            # @maxim 12.12 11:11 500р каналбизнес (без формата)
            r'@(\w+)\s+(\d{1,2}\.\d{1,2})\s+(\d{1,2}:\d{2})\s+(\d+(?:\.\d+)?)(usdt|р|руб|\$|₽|юсдт)\s+(.+)',
            # @maxim 12/12 11:11 500р каналбизнес (без формата)
            r'@(\w+)\s+(\d{1,2}/\d{1,2})\s+(\d{1,2}:\d{2})\s+(\d+(?:\.\d+)?)(usdt|р|руб|\$|₽|юсдт)\s+(.+)',
            # @maxim 12-12 11:11 500р каналбизнес (без формата)
            r'@(\w+)\s+(\d{1,2}-\d{1,2})\s+(\d{1,2}:\d{2})\s+(\d+(?:\.\d+)?)(usdt|р|руб|\$|₽|юсдт)\s+(.+)',
            # @maxim 12 декабря 11:11 500р каналбизнес (без usdt/$)
            r'@(\w+)\s+(\d{1,2}\s+\w+)\s+(\d{1,2}:\d{2})\s+(\d+(?:\.\d+)?)(р|руб|₽)\s+(.+)',
            # @maxim 12.12 11:11 500р каналбизнес (без usdt/$)
            r'@(\w+)\s+(\d{1,2}\.\d{1,2})\s+(\d{1,2}:\d{2})\s+(\d+(?:\.\d+)?)(р|руб|₽)\s+(.+)',
            # @bob 12 янв 1634 888юсдт СОсалово (время без двоеточия)
            r'@(\w+)\s+(\d{1,2}\s+\w+)\s+(\d{1,2}\d{2})\s+(\d+(?:\.\d+)?)(usdt|р|руб|\$|₽|юсдт)\s+(.+)',
            # @bob 12.01 1634 888юсдт СОсалово (дата с точкой, время без двоеточия)
            r'@(\w+)\s+(\d{1,2}\.\d{1,2})\s+(\d{1,2}\d{2})\s+(\d+(?:\.\d+)?)(usdt|р|руб|\$|₽|юсдт)\s+(.+)',
            # @charlie 10/03 915 2000юсдт НовыйКанал (время с ведущим нулем)
            r'@(\w+)\s+(\d{1,2}/\d{1,2})\s+(\d{1,2}\d{1,2})\s+(\d+(?:\.\d+)?)(usdt|р|руб|\$|₽|юсдт)\s+(.+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                manager = match.group(1)
                
                # Проверяем, какой это формат по количеству групп и содержимому
                if len(match.groups()) == 7 and match.group(6) and '/' in str(match.group(6)):
                    # Проверяем, есть ли пробел в имени менеджера (новый формат)
                    if ' ' in manager:
                        # Новый формат без @ (Ксения Вантрип 1230 16.04 501юсдт 1/24 БиБ)
                        time_str = match.group(2)
                        date_str = match.group(3)
                        amount = float(match.group(4))
                        currency = match.group(5).lower()
                        format_str = match.group(6)
                        channel = match.group(7).strip()
                    else:
                        # Формат с @ и с форматом (7 групп)
                        date_str = match.group(2)
                        time_str = match.group(3)
                        amount = float(match.group(4))
                        currency = match.group(5).lower()
                        format_str = match.group(6)
                        channel = match.group(7).strip()
                elif len(match.groups()) == 6:
                    # Формат с @ без формата (6 групп)
                    date_str = match.group(2)
                    time_str = match.group(3)
                    amount = float(match.group(4))
                    currency = match.group(5).lower()
                    format_str = ""
                    channel = match.group(6).strip()
                else:
                    # Новый формат без @ (Ксения Вантрип 1230 16.04 501юсдт 1/24 БиБ)
                    time_str = match.group(2)
                    date_str = match.group(3)
                    amount = float(match.group(4))
                    currency = match.group(5).lower()
                    format_str = match.group(6)
                    channel = match.group(7).strip()
                
                # Всегда добавляем @ к имени менеджера для единообразия
                manager = f"@{manager}"
                
                # Нормализация времени (добавляем двоеточие если его нет)
                if ':' not in time_str:
                    if len(time_str) == 4:
                        time_str = f"{time_str[:2]}:{time_str[2:]}"
                    elif len(time_str) == 3:
                        time_str = f"0{time_str[0]}:{time_str[1:]}"
                
                # Нормализация валюты
                if currency in ['р', 'руб', '₽']:
                    currency = 'RUB'
                elif currency in ['usdt', '$', 'юсдт']:
                    currency = 'USDT'
                else:
                    # Если валюта не указана явно, пытаемся определить по контексту
                    if 'usdt' in text.lower() or '$' in text or 'юсдт' in text.lower():
                        currency = 'USDT'
                    else:
                        currency = 'RUB'  # По умолчанию рубли
                
                # Парсинг даты
                try:
                    if '.' in date_str:
                        # Формат 14.05 или 12.12
                        day, month = date_str.split('.')
                        current_year = datetime.now().year
                        parsed_date = datetime(current_year, int(month), int(day))
                    elif '/' in date_str:
                        # Формат 14/05 или 12/12
                        day, month = date_str.split('/')
                        current_year = datetime.now().year
                        parsed_date = datetime(current_year, int(month), int(day))
                    elif '-' in date_str:
                        # Формат 14-05 или 12-12
                        day, month = date_str.split('-')
                        current_year = datetime.now().year
                        parsed_date = datetime(current_year, int(month), int(day))
                    else:
                        # Формат "12 декабря" или "12 янв"
                        month_names = {
                            # Полные названия
                            'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
                            'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
                            'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12,
                            # Сокращенные названия
                            'янв': 1, 'фев': 2, 'мар': 3, 'апр': 4,
                            'май': 5, 'июн': 6, 'июл': 7, 'авг': 8,
                            'сен': 9, 'окт': 10, 'ноя': 11, 'дек': 12
                        }
                        parts = date_str.split()
                        day = int(parts[0])
                        month = month_names.get(parts[1].lower(), 1)
                        current_year = datetime.now().year
                        parsed_date = datetime(current_year, month, day)
                    
                    return {
                        'manager': manager,
                        'date': parsed_date.strftime('%d.%m.%Y'),
                        'time': time_str,
                        'amount': amount,
                        'currency': currency,
                        'format': format_str,
                        'channel': channel
                    }
                except Exception as e:
                    logger.error(f"Ошибка парсинга даты: {e}")
                    continue
        
        return None
    
    def _validate_format(self, format_str: str) -> bool:
        """Валидация формата - принимаются только 1/24 или 1/48"""
        if not format_str:
            return True  # Пустой формат допустим
        
        valid_formats = ['1/24', '1/48']
        return format_str in valid_formats
    
    def _add_to_sheets(self, data: Dict):
        """Добавление данных в Google Sheets"""
        try:
            # Форматируем сумму с пробелами для тысяч
            amount_str = self._format_amount(data['amount'])
            
            # Формируем данные в нужном формате
            # Покупатель, Дата, Время, Сумма, Валюта, Формат, Канал где была публикация
            row = [
                data['manager'],  # Покупатель (без @, так как @ добавляется в парсере)
                data['date'],  # Дата отдельно
                data['time'],  # Время отдельно
                amount_str,  # Сумма с пробелами для тысяч
                data['currency'],  # Валюта
                data.get('format', ''),  # Формат (может быть пустым)
                data['channel']  # Канал
            ]
            
            if self.sheet:
                self.sheet.append_row(row)
                logger.info(f"Данные добавлены в таблицу: {data}")
            else:
                logger.info(f"Данные записаны в режиме симуляции: {data}")
                logger.info(f"Строка для Google Sheets: {row}")
            
        except Exception as e:
            logger.error(f"Ошибка добавления в Google Sheets: {e}")
            raise
    
    def _format_amount(self, amount: float) -> str:
        """Форматирование суммы с пробелами для тысяч"""
        if amount.is_integer():
            # Для целых чисел добавляем пробелы для тысяч
            amount_int = int(amount)
            return f"{amount_int:,}".replace(",", " ")
        else:
            # Для дробных чисел
            return f"{amount:,.2f}".replace(",", " ").replace(".00", "")
    
    def _update_stats(self, data: Dict):
        """Обновление статистики"""
        self.stats['total_sales'] += 1
        
        if data['currency'] == 'USDT':
            self.stats['total_usdt'] += data['amount']
        elif data['currency'] == 'RUB':
            self.stats['total_rub'] += data['amount']
        
        # Обновляем статистику по методам оплаты
        payment_key = f"{data['currency']}"
        self.stats['sales_by_payment'][payment_key] = self.stats['sales_by_payment'].get(payment_key, 0) + 1
    
    def _handle_sales_message(self, message):
        """Обработчик сообщений о продажах"""
        text = message.text.strip()
        
        # Парсим сообщение
        parsed_data = self._parse_sales_message(text)
        
        if parsed_data:
            # Валидируем формат
            if not self._validate_format(parsed_data.get('format', '')):
                self.bot.send_message(
                    message.chat.id,
                    "❌ <b>Ошибка валидации формата!</b>\n\n"
                    "Принимаются только следующие форматы:\n"
                    "• <code>1/24</code>\n"
                    "• <code>1/48</code>\n\n"
                    "Другие значения не принимаются.",
                    parse_mode='HTML'
                )
                return
            
            try:
                # Добавляем в Google Sheets
                self._add_to_sheets(parsed_data)
                
                # Обновляем статистику
                self._update_stats(parsed_data)
                
                # Создаем клавиатуру с ссылкой на таблицу
                keyboard = types.InlineKeyboardMarkup()
                keyboard.add(types.InlineKeyboardButton(
                    "📊 Открыть таблицу", 
                    url=f"https://docs.google.com/spreadsheets/d/{self.sheets_id}"
                ))
                
                # Отправляем подтверждение
                confirmation_text = f"""
✅ <b>Данные занесены в учет!</b>

👤 <b>Менеджер:</b> {parsed_data['manager']}
📅 <b>Дата:</b> {parsed_data['date']}
🕐 <b>Время:</b> {parsed_data['time']}
💰 <b>Сумма:</b> {parsed_data['amount']} {parsed_data['currency']}
📋 <b>Формат:</b> {parsed_data.get('format', 'Не указан')}
📺 <b>Канал:</b> {parsed_data['channel']}
                """
                
                self.bot.send_message(
                    message.chat.id,
                    confirmation_text,
                    parse_mode='HTML',
                    reply_markup=keyboard
                )
                
            except Exception as e:
                logger.error(f"Ошибка обработки сообщения: {e}")
                self.bot.send_message(
                    message.chat.id,
                    "❌ Произошла ошибка при обработке данных. Попробуйте еще раз."
                )
        else:
            # Если сообщение не распознано как продажа
            self.bot.send_message(
                message.chat.id,
                "❓ Не удалось распознать формат сообщения.\n\n"
                "Используйте формат:\n"
                "<code>@менеджер дата время сумма [формат] канал</code>\n\n"
                "Примеры:\n"
                "• <code>@maxim 12 декабря 11:11 1489usdt 1/24 BusinessChannel</code>\n"
                "• <code>@anna 14.05 11:11 500р каналбизнес</code>\n\n"
                "<b>Доступные форматы:</b> 1/24, 1/48",
                parse_mode='HTML'
            )
    
    def run(self):
        """Запуск бота"""
        logger.info("Запуск бота...")
        try:
            self.bot.polling(none_stop=True, interval=0)
        except Exception as e:
            logger.error(f"Ошибка запуска бота: {e}")
            raise

def signal_handler(signum, frame):
    """Обработчик сигналов для graceful shutdown"""
    logger.info(f"Получен сигнал {signum}. Завершение работы...")
    sys.exit(0)

if __name__ == "__main__":
    # Регистрируем обработчики сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        logger.info("Запуск Sales Bot...")
        bot = SalesBot()
        bot.run()
    except KeyboardInterrupt:
        logger.info("Получен сигнал прерывания. Завершение работы...")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        sys.exit(1)
