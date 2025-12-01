import os
import re
import logging
import signal
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from io import BytesIO

import telebot
from telebot import types
import gspread
from google.oauth2.service_account import Credentials

# Рендеринг графиков без дисплея
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter
except Exception:
    plt = None

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
            # Небольшая задержка для завершения операции
            import time
            time.sleep(1)
        except Exception as e:
            logger.warning(f"Не удалось снять webhook: {e}")
        self.sheets_id = config.GOOGLE_SHEETS_ID
        logger.info(f"Google Sheets ID из конфига: {self.sheets_id}")
        self.sheet = None
        self.stats = {
            'total_usdt': 0,
            'total_rub': 0,
            'total_sales': 0,
            'sales_by_payment': {}
        }

        # Разделители комментария после названия канала
        self.comment_delimiters = [' -- ', ' — ', ' – ', ' | ', ' / ', '  ']
        # Ключевые слова/триггеры, с которых часто начинается комментарий
        self.comment_keywords = [
            'мб', 'может', 'возможно', 'вероятно', 'наверн', 'скорее',
            'коммент', 'комментар', 'примечан', 'замет', 'note', 'comment',
            'ещё', 'еще', 'купит', 'доп', 'доп.', '+', 'потом'
        ]

        # Нормализация названий каналов
        self.channel_aliases = {
            # русские варианты и сокращения → каноническое имя
            'русский бизнес': 'Русский бизнес | Стартапы',
            'русский  бизнес': 'Русский бизнес | Стартапы',
            'русский-бизнес': 'Русский бизнес | Стартапы',
            'рб': 'Русский бизнес | Стартапы',
            'rb': 'Русский бизнес | Стартапы',
            'русбизнес': 'Русский бизнес | Стартапы',
            # Нормализация для новых каналов
            'источник': 'Источник',
            'источник экономика': 'Источник | Экономика',
            'источник | экономика': 'Источник | Экономика',
            'источник | экономику': 'Источник | Экономика',
            'источник экономику': 'Источник | Экономика',
            'экономика': 'Источник | Экономика',
            'валютный банк': 'Источник | Экономика'
        }

        # Нормализация типов оплаты
        self.payment_type_aliases = {
            'сбп': 'СБП',
            'карта': 'Карта',
            'крипта': 'Криптовалюта',
            'криптовалюта': 'Криптовалюта',
            'ип': 'ИП'
        }

        # Нормализация внешняя/внутренняя
        self.internal_external_aliases = {
            'внешка': 'Внешняя',
            'внешняя': 'Внешняя',
            'внутренняя': 'Внутренняя',
            'внутреняя': 'Внутренняя',
            'внутренний': 'Внутренняя',
            'внутрянка': 'Внутренняя'
        }
        
        # Настройка Google Sheets
        self._setup_google_sheets()
        
        # Регистрация обработчиков
        self._register_handlers()

    def _split_channel_and_comment(self, channel_with_comment: str):
        """Отделяет комментарий от названия канала по известным разделителям.
        Возвращает (channel, comment). Если комментария нет — возвращает comment=''"""
        text = channel_with_comment.strip()
        for delim in self.comment_delimiters:
            if delim in text:
                parts = text.split(delim, 1)
                return self._normalize_channel_name(parts[0].strip()), parts[1].strip()
        # Если специальных разделителей нет — пробуем по ключевым словам комментария
        lowered = text.lower()
        keyword_positions = [lowered.find(' ' + kw) for kw in self.comment_keywords]
        keyword_positions = [pos for pos in keyword_positions if pos > 0]
        if keyword_positions:
            split_pos = min(keyword_positions)
            return self._normalize_channel_name(text[:split_pos].strip()), text[split_pos:].strip()
        # Если ничего не нашли — считаем, что комментария нет
        return self._normalize_channel_name(text), ''

    def _normalize_channel_name(self, channel_name: str) -> str:
        """Нормализует название канала по словарю синонимов/алиасов."""
        key = channel_name.strip().lower()
        # Убираем лишние пробелы/дефисы для сравнения
        compact_key = key.replace('  ', ' ').replace('-', ' ').strip()
        compact_key = ' '.join(compact_key.split())
        if key in self.channel_aliases:
            return self.channel_aliases[key]
        if compact_key in self.channel_aliases:
            return self.channel_aliases[compact_key]
        return channel_name

    def _normalize_payment_type(self, payment_type: str) -> str:
        """Нормализует тип оплаты."""
        if not payment_type:
            return ""
        key = payment_type.strip().lower()
        return self.payment_type_aliases.get(key, payment_type)

    def _normalize_internal_external(self, internal_external: str) -> str:
        """Нормализует внешняя/внутренняя."""
        if not internal_external:
            return ""
        key = internal_external.strip().lower()
        return self.internal_external_aliases.get(key, internal_external)

    def _send_notification(self, data: Dict):
        """Отправка уведомления о продаже в другой чат/топик"""
        if not config.NOTIFICATION_CHAT_ID:
            logger.info("NOTIFICATION_CHAT_ID не настроен")
            return
        
        logger.info(f"=== ОТПРАВКА УВЕДОМЛЕНИЯ ===")
        logger.info(f"NOTIFICATION_CHAT_ID: {config.NOTIFICATION_CHAT_ID}")
        
        try:
            # Проверяем, является ли менеджер @mqwou
            manager_username = data.get('manager_username', 'unknown')
            if manager_username == 'mqwou':
                # Специальный формат для @mqwou с комиссией 5%
                commission = data['amount'] * 0.05
                final_amount = data['amount'] - commission
                notification_text = f"""
✅ <b>Новая продажа на {data['amount']} {data['currency']} от менеджера @{manager_username}</b>

👤 <b>Покупатель:</b> {data['manager']}
📅 <b>Дата:</b> {data['date']}
🕐 <b>Время:</b> {data['time']}
💰 <b>Сумма:</b> {data['amount']} {data['currency']} - 5% комиссия = {final_amount:.2f} {data['currency']}
💳 <b>Тип оплаты:</b> {data.get('payment_type', 'Не указан')}
📋 <b>Формат:</b> {data.get('format', 'Не указан')}
🏢 <b>Внешняя/Внутренняя:</b> {data.get('internal_external', 'Не указано')}
                """
            else:
                # Обычный формат для других менеджеров
                notification_text = f"""
✅ <b>Новая продажа на {data['amount']} {data['currency']} от менеджера @{manager_username}</b>

👤 <b>Покупатель:</b> {data['manager']}
📅 <b>Дата:</b> {data['date']}
🕐 <b>Время:</b> {data['time']}
💰 <b>Сумма:</b> {data['amount']} {data['currency']}
💳 <b>Тип оплаты:</b> {data.get('payment_type', 'Не указан')}
📋 <b>Формат:</b> {data.get('format', 'Не указан')}
🏢 <b>Внешняя/Внутренняя:</b> {data.get('internal_external', 'Не указано')}
📺 <b>Канал:</b> {data['channel']}
💬 <b>Комментарий:</b> {data.get('comment', 'Нет')}
                """
            
            # Проверяем, есть ли ID топика в переменной
            if '#' in config.NOTIFICATION_CHAT_ID:
                # Отправляем в топик
                chat_id, topic_id = config.NOTIFICATION_CHAT_ID.split('#')
                logger.info(f"Отправляем в топик: chat_id={chat_id}, topic_id={topic_id}")
                
                self.bot.send_message(
                    chat_id,
                    notification_text,
                    parse_mode='HTML',
                    message_thread_id=int(topic_id)
                )
                logger.info(f"✅ Уведомление отправлено в топик {config.NOTIFICATION_CHAT_ID}")
            else:
                # Отправляем в обычный чат
                logger.info(f"Отправляем в чат: {config.NOTIFICATION_CHAT_ID}")
                
                self.bot.send_message(
                    config.NOTIFICATION_CHAT_ID,
                    notification_text,
                    parse_mode='HTML'
                )
                logger.info(f"✅ Уведомление отправлено в чат {config.NOTIFICATION_CHAT_ID}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка отправки уведомления: {e}")
            logger.error(f"NOTIFICATION_CHAT_ID: {config.NOTIFICATION_CHAT_ID}")
        
        logger.info(f"================================")
        
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
            
            # Открываем таблицу и выбираем/создаем вкладку "Декабрь"
            logger.info(f"Открываем Google Sheets с ID: {self.sheets_id}")
            spreadsheet = gc.open_by_key(self.sheets_id)
            self.spreadsheet = spreadsheet
            self.sheet = self._ensure_december_sheet(self.spreadsheet)
            
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
            logger.error(f"❌ Не удалось подключиться к Google Sheets: {e}")
            logger.error(f"Проверьте: 1) GOOGLE_SHEETS_ID={self.sheets_id}, 2) credentials.json, 3) доступ к таблице")
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
        
        @self.bot.message_handler(commands=['resetstats'])
        def reset_stats_command(message):
            self._handle_reset_stats(message)
        
        @self.bot.message_handler(commands=['money'])
        def money_command(message):
            self._handle_money(message)

        @self.bot.callback_query_handler(func=lambda call: call.data.startswith('money_month:'))
        def money_month_callback(call):
            try:
                month_title = call.data.split(':', 1)[1]
                self._handle_money(call.message, month_title_override=month_title)
                self.bot.answer_callback_query(call.id)
            except Exception as e:
                logger.error(f"Ошибка обработки выбора месяца: {e}")
                self.bot.answer_callback_query(call.id, text="Ошибка")
        
        @self.bot.message_handler(commands=['debug'])
        def debug_command(message):
            self._handle_debug(message)
        
        @self.bot.message_handler(func=lambda message: True)
        def handle_message(message):
            self._handle_sales_message(message)

    def _ensure_december_sheet(self, spreadsheet):
        """Гарантирует наличие и возврат листа 'Декабрь'"""
        try:
            sheet = spreadsheet.worksheet('Декабрь')
            return sheet
        except gspread.WorksheetNotFound:
            logger.info("Вкладка 'Декабрь' не найдена. Создаем новую вкладку...")
            sheet = spreadsheet.add_worksheet(title='Декабрь', rows=1000, cols=10)
            return sheet

    def _init_sheets(self):
        """Инициализирует self.sheet для листа 'Декабрь' если не инициализировано или потеряно"""
        try:
            import json
            creds_env = os.getenv('GOOGLE_CREDENTIALS_JSON')
            if creds_env:
                try:
                    creds_data = json.loads(creds_env)
                except json.JSONDecodeError:
                    creds_data = json.loads(creds_env.replace('\\n', '\n'))
            else:
                creds_file = config.CREDENTIALS_FILE
                if not os.path.exists(creds_file):
                    creds_file = os.path.join(config.CREDENTIALS_FOLDER, 'credentials.json')
                if not os.path.exists(creds_file):
                    logger.info("credentials.json не найден — пропускаем повторную инициализацию")
                    return
                with open(creds_file, 'r', encoding='utf-8') as f:
                    creds_data = json.load(f)
            # нормализуем ключ и создаем клиента
            if isinstance(creds_data.get('private_key'), str):
                creds_data['private_key'] = creds_data['private_key'].replace('\\n', '\n')
            creds = Credentials.from_service_account_info(creds_data, scopes=config.SHEET_SCOPE)
            gc = gspread.authorize(creds)
            spreadsheet = gc.open_by_key(self.sheets_id)
            self.spreadsheet = spreadsheet
            self.sheet = self._ensure_december_sheet(self.spreadsheet)
        except Exception as e:
            logger.warning(f"_init_sheets: не удалось переинициализировать Google Sheets: {e}")
    
    def _handle_start(self, message):
        """Обработчик команды /start"""
        # Временное логирование для получения ID
        logger.info(f"=== ID ЧАТА ===")
        logger.info(f"Chat ID: {message.chat.id}")
        logger.info(f"Chat Type: {message.chat.type}")
        if hasattr(message, 'message_thread_id') and message.message_thread_id:
            logger.info(f"Topic ID: {message.message_thread_id}")
            logger.info(f"Full ID: {message.chat.id}#{message.message_thread_id}")
        logger.info(f"===============")
        
        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(types.InlineKeyboardButton(
            "📊 Открыть таблицу", 
            url=f"https://docs.google.com/spreadsheets/d/{self.sheets_id}"
        ))
        
        welcome_text = f"""
👾 <b>Бот для учета продажи рекламы Dox Media</b>

// <b>Как использовать:</b>
Отправьте сообщение в формате:
• <code>@кому продали (или без @, просто имя фамилия) дата время сумма тип_оплаты формат внешняя/внутренняя канал / комментарий</code>

// <b>Примеры:</b>
• <code>Максим Шариков 12.06 1215 500р сбп 1/48 внешка русский бизнес / вероятно купят еще</code>
• <code>Максим Шариков 12.06 1215 500р ип 1/48 внутренняя русский бизнес / вероятно купят еще</code>
• <code>Максим Шариков 12.06 1215 500р крипта внешка 1/48 русский бизнес / вероятно купят еще</code>

// <b>Доступные команды:</b>
/start — Главное меню
/stats — Статистика продаж
/money — Финансовая статистика
/debug — Отладка таблицы

// <b>ID чата:</b> <code>{message.chat.id}</code>
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
    
    def _handle_reset_stats(self, message):
        """Обнуление статистики"""
        self.stats = {
            'total_usdt': 0,
            'total_rub': 0,
            'total_sales': 0,
            'sales_by_payment': {}
        }
        self.bot.send_message(
            message.chat.id,
            "✅ Статистика обнулена. Используйте /stats для просмотра.",
            parse_mode='HTML'
        )
    
    def _handle_money(self, message, month_title_override: Optional[str] = None):
        """Обработчик команды /money - финансовая статистика из таблицы"""
        try:
            if not self.sheet:
                self._init_sheets()
            
            # Выбор листа: по умолчанию 'Декабрь' или по клику пользователя
            target_title = month_title_override or 'Декабрь'
            if hasattr(self, 'spreadsheet') and self.spreadsheet:
                try:
                    target_sheet = self.spreadsheet.worksheet(target_title)
                except gspread.WorksheetNotFound:
                    target_sheet = None
            else:
                target_sheet = None
            
            # Если нет нужного листа — показываем выбор доступных
            if not target_sheet and hasattr(self, 'spreadsheet') and self.spreadsheet:
                months = [ws.title for ws in self.spreadsheet.worksheets()]
                keyboard = types.InlineKeyboardMarkup()
                # первые 12
                for title in months[:12]:
                    keyboard.add(types.InlineKeyboardButton(title, callback_data=f"money_month:{title}"))
                self.bot.send_message(
                    message.chat.id,
                    "Выберите лист (месяц) для статистики:",
                    reply_markup=keyboard
                )
                return

            # Получаем финансовые данные из выбранного листа
            financial_data = self._get_financial_data(target_sheet)
            
            if not financial_data:
                self.bot.send_message(
                    message.chat.id,
                    "❌ Не удалось получить финансовые данные из таблицы",
                    parse_mode='HTML'
                )
                return
            
            # Формируем сообщение
            money_text = f"""
💰 <b>Финансовая статистика</b>

📄 <b>Лист:</b> {target_title}

💵 <b>Выручка:</b>
• USDT: {financial_data.get('revenue_usdt', 0):.2f}
• RUB: {financial_data.get('revenue_rub', 0):,.0f}
Суммарная выручка: {financial_data.get('total_revenue', 0):,.2f}

💸 <b>Чистыми заработано:</b>
• USDT: {financial_data.get('net_usdt', 0):.2f}
• RUB: {financial_data.get('net_rub', 0):,.0f}
Суммарная прибыль: {financial_data.get('total_profit', 0):,.2f}

💼 <b>Комиссии по сейлзам:</b>

👨‍💼 <b>Дима (5%):</b>
• USDT: {financial_data.get('dima_commission_usdt', 0):.2f}
• RUB: {financial_data.get('dima_commission_rub', 0):,.0f}

👩‍💼 <b>Алина (15%):</b>
• USDT: {financial_data.get('alina_commission_usdt', 0):.2f}
• RUB: {financial_data.get('alina_commission_rub', 0):,.0f}

👩‍💼 <b>Ксения (10%):</b>
• USDT: {financial_data.get('ksenia_commission_usdt', 0):.2f}
• RUB: {financial_data.get('ksenia_commission_rub', 0):,.0f}

👨‍💼 <b>Роман (10%):</b>
• USDT: {financial_data.get('roman_commission_usdt', 0):.2f}
• RUB: {financial_data.get('roman_commission_rub', 0):,.0f}

💳 <b>По типам оплаты:</b>
• СБП: {financial_data.get('sbp_count', 0)}
• Карта: {financial_data.get('card_count', 0)}
• Крипта: {financial_data.get('crypto_count', 0)}
• ИП: {financial_data.get('ip_count', 0)}
            """
            
            # Клавиатура: открыть таблицу + выбрать месяц
            keyboard = types.InlineKeyboardMarkup()
            keyboard.add(types.InlineKeyboardButton(
                "📊 Открыть таблицу", 
                url=f"https://docs.google.com/spreadsheets/d/{self.sheets_id}"
            ))
            if hasattr(self, 'spreadsheet') and self.spreadsheet:
                months = [ws.title for ws in self.spreadsheet.worksheets()]
                # compact rows of buttons
                row = []
                for title in months[:12]:
                    row.append(types.InlineKeyboardButton(title, callback_data=f"money_month:{title}"))
                    if len(row) == 3:
                        keyboard.row(*row)
                        row = []
                if row:
                    keyboard.row(*row)
            
            # Если доступен matplotlib — рендерим сводный дэшборд 2x2 и отправляем как фото с подписью
            if plt:
                try:
                    # Загружаем все строки из выбранного листа (A:J)
                    all_values = target_sheet.get_all_values()
                    if len(all_values) < 2:
                        # Нет данных — отправляем текст
                        self.bot.send_message(
                            message.chat.id,
                            money_text,
                            parse_mode='HTML',
                            reply_markup=keyboard
                        )
                        return

                    header = all_values[0]
                    rows = all_values[1:]

                    # Индексы колонок согласно записи бота A:J
                    IDX_DATE = 1   # 'Дата' в формате dd.mm.YYYY
                    IDX_TIME = 2   # 'Время' HH:MM
                    IDX_AMOUNT = 3 # float
                    IDX_CURRENCY = 4
                    IDX_PAYMENT = 5
                    IDX_CHANNEL = 8

                    # Агрегации
                    from collections import defaultdict, Counter
                    # 1) Ежедневная выручка по валютам
                    daily_usdt = defaultdict(float)
                    daily_rub = defaultdict(float)
                    # 2) Микс способов оплаты (кол-во)
                    payment_counts = Counter()
                    # 3) Теплокарта День×Час
                    heat = [[0 for _ in range(24)] for _ in range(7)]  # 0=Mon ... 6=Sun
                    # 4) Pareto каналов по выручке (общая сумма в RUB-эквиваленте? показываем раздельно, но для сортировки — сумма в своей валюте отдельно не сопоставима. Выберем просто по количеству строк)
                    channel_revenue = defaultdict(float)

                    # Функции парсинга
                    def parse_float_safe(s: str) -> float:
                        try:
                            return float(str(s).replace(' ', '').replace('\xa0', '').replace('₽', '').replace(',', ''))
                        except Exception:
                            return 0.0

                    def parse_hour_safe(t: str) -> int:
                        t = (t or '').strip()
                        if not t:
                            return 0
                        if ':' not in t:
                            if len(t) == 4:
                                t = f"{t[:2]}:{t[2:]}"
                            elif len(t) == 3:
                                t = f"0{t[0]}:{t[1:]}"
                        try:
                            return int(t.split(':')[0])
                        except Exception:
                            return 0

                    def parse_dow(date_str: str) -> int:
                        # ожидаем dd.mm.YYYY
                        try:
                            day, month, year = date_str.split('.')
                            dt = datetime(int(year), int(month), int(day))
                            return dt.weekday()  # 0-6
                        except Exception:
                            return 0

                    for r in rows:
                        try:
                            date_str = r[IDX_DATE] if len(r) > IDX_DATE else ''
                            time_str = r[IDX_TIME] if len(r) > IDX_TIME else ''
                            amount = parse_float_safe(r[IDX_AMOUNT] if len(r) > IDX_AMOUNT else '')
                            currency = (r[IDX_CURRENCY] if len(r) > IDX_CURRENCY else '').strip().upper()
                            payment = (r[IDX_PAYMENT] if len(r) > IDX_PAYMENT else '').strip()
                            channel = (r[IDX_CHANNEL] if len(r) > IDX_CHANNEL else '').strip()

                            if amount <= 0 or not date_str:
                                continue

                            # 1) daily by currency
                            if currency == 'USDT':
                                daily_usdt[date_str] += amount
                            elif currency == 'RUB':
                                daily_rub[date_str] += amount

                            # 2) payment mix
                            payment_key = payment if payment else 'Не указан'
                            payment_counts[payment_key] += 1

                            # 3) heatmap
                            dow = parse_dow(date_str)
                            hour = parse_hour_safe(time_str)
                            if 0 <= dow <= 6 and 0 <= hour <= 23:
                                heat[dow][hour] += 1

                            # 4) channel pareto (по сумме без конвертации)
                            channel_revenue[channel or '—'] += amount
                        except Exception as parse_e:
                            logger.debug(f"skip row due to parse error: {parse_e}")

                    # Подготовка фигур
                    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
                    fig.suptitle('Сводная аналитика', fontsize=14)
                    plt.subplots_adjust(hspace=0.35, wspace=0.25)

                    # A) Ежедневная выручка RUB (отдельная диаграмма)
                    dates_sorted = sorted(set(list(daily_usdt.keys()) + list(daily_rub.keys())), key=lambda d: datetime.strptime(d, '%d.%m.%Y'))
                    usdt_vals = [daily_usdt.get(d, 0) for d in dates_sorted]
                    rub_vals = [daily_rub.get(d, 0) for d in dates_sorted]
                    x = range(len(dates_sorted))
                    axes[0,0].bar(x, rub_vals, color='#f28e2b')
                    axes[0,0].set_title('Выручка по дням (RUB)')
                    axes[0,0].set_xticks(list(x))
                    axes[0,0].set_xticklabels([d[:-5] for d in dates_sorted], rotation=30)

                    # B) Ежедневная выручка USDT (отдельная диаграмма)
                    axes[0,1].bar(x, usdt_vals, color='#4e79a7')
                    axes[0,1].set_title('Выручка по дням (USDT)')
                    axes[0,1].set_xticks(list(x))
                    axes[0,1].set_xticklabels([d[:-5] for d in dates_sorted], rotation=30)

                    # C) Теплокарта активностей (День×Час)
                    im = axes[1,0].imshow(heat, aspect='auto', cmap='YlOrRd')
                    axes[1,0].set_title('Активность: дни×часы')
                    axes[1,0].set_yticks(range(7))
                    axes[1,0].set_yticklabels(['Пн','Вт','Ср','Чт','Пт','Сб','Вс'])
                    axes[1,0].set_xticks([0,4,8,12,16,20,23])
                    axes[1,0].set_xticklabels(['0','4','8','12','16','20','23'])
                    fig.colorbar(im, ax=axes[1,0], fraction=0.046, pad=0.04)

                    # D) Топ-каналы: бары + кумулятив
                    top_items = sorted(channel_revenue.items(), key=lambda kv: kv[1], reverse=True)[:10]
                    labels_d = [k if k else '—' for k,_ in top_items]
                    vals_d = [v for _,v in top_items]
                    if vals_d:
                        x2 = range(len(vals_d))
                        bars = axes[1,1].bar(x2, vals_d, color='#59a14f')
                        axes[1,1].set_title('Топ-каналы')
                        axes[1,1].set_xticks(list(x2))
                        axes[1,1].set_xticklabels(labels_d, rotation=30, ha='right')
                        # Кумулятивная линия от 0 до 100%
                        total = sum(vals_d)
                        cum = []
                        s = 0
                        for v in vals_d:
                            s += v
                            cum.append(s / total if total > 0 else 0)
                        ax2 = axes[1,1].twinx()
                        ax2.plot(list(x2), [c*100 for c in cum], color='#e15759', marker='o')
                        ax2.yaxis.set_major_formatter(PercentFormatter())
                        ax2.set_ylim(0, 105)
                        ax2.grid(False)

                    buf = BytesIO()
                    fig.savefig(buf, format='png', dpi=180, bbox_inches='tight')
                    plt.close(fig)
                    buf.seek(0)

                    # Отправляем как фото с подписью (caption)
                    self.bot.send_photo(
                        message.chat.id,
                        buf,
                        caption=money_text,
                        parse_mode='HTML',
                        reply_markup=keyboard
                    )
                    buf.close()
                except Exception as e:
                    logger.warning(f"Не удалось отрисовать диаграмму: {e}")
                    # Фоллбек: просто текст если график не собрался
                    self.bot.send_message(
                        message.chat.id,
                        money_text,
                        parse_mode='HTML',
                        reply_markup=keyboard
                    )
            else:
                # Если matplotlib недоступен — отправляем текст
                self.bot.send_message(
                    message.chat.id,
                    money_text,
                    parse_mode='HTML',
                    reply_markup=keyboard
                )
            
        except Exception as e:
            logger.error(f"Ошибка получения финансовых данных: {e}")
            self.bot.send_message(
                message.chat.id,
                f"❌ Ошибка получения данных: {str(e)}",
                parse_mode='HTML'
            )
    
    def _get_financial_data(self, sheet) -> Dict:
        """Получение финансовых данных из указанного листа таблицы"""
        try:
            if not sheet:
                return {}
            
            # Получаем все данные из таблицы
            all_values = sheet.get_all_values()
            
            if len(all_values) < 2:  # Только заголовки
                return {}
            
            # Ищем строки с финансовыми данными
            financial_data = {
                'revenue_usdt': 0,
                'revenue_rub': 0,
                'net_usdt': 0,
                'net_rub': 0,
                'sbp_count': 0,
                'card_count': 0,
                'crypto_count': 0,
                'ip_count': 0,
                # Комиссии по сейлзам
                'dima_commission_usdt': 0,
                'dima_commission_rub': 0,
                'alina_commission_usdt': 0,
                'alina_commission_rub': 0,
                'ksenia_commission_usdt': 0,
                'ksenia_commission_rub': 0,
                'roman_commission_usdt': 0,
                'roman_commission_rub': 0
            }
            
            # Добавляем отладочное логирование
            logger.info(f"Всего строк в таблице: {len(all_values)}")
            
            # Проходим по всем строкам и ищем финансовые данные
            for i, row in enumerate(all_values):
                logger.info(f"Строка {i}: {row}")
                
                if len(row) >= 22:  # Проверяем, что строка достаточно длинная (до колонки V)
                    # Ищем строки с валютами USDT и RUB в колонке L (индекс 11)
                    if len(row) > 11 and row[11] in ['USDT', 'RUB']:
                        currency = row[11]
                        logger.info(f"Найдена валюта {currency} в строке {i}")
                        logger.info(f"Полная строка: {row}")
                        logger.info(f"Колонка O (индекс 14): '{row[14] if len(row) > 14 else 'НЕТ'}'")
                        logger.info(f"Колонка Q (индекс 16): '{row[16] if len(row) > 16 else 'НЕТ'}'")
                        
                        # Выручка (колонка M, индекс 12)
                        if len(row) > 12 and row[12]:
                            try:
                                revenue_str = row[12].replace(' ', '').replace('\xa0', '').replace('₽', '').replace(',', '.')
                                revenue = float(revenue_str)
                                if currency == 'USDT':
                                    financial_data['revenue_usdt'] = revenue
                                elif currency == 'RUB':
                                    financial_data['revenue_rub'] = revenue
                                logger.info(f"Выручка {currency}: {revenue}")
                            except (ValueError, IndexError) as e:
                                logger.warning(f"Ошибка парсинга выручки: {e}, значение: {row[12]}")
                        
                        # Чистыми заработано (колонка N, индекс 13)
                        if len(row) > 13 and row[13]:
                            try:
                                net_str = row[13].replace(' ', '').replace('\xa0', '').replace('₽', '').replace(',', '.')
                                net = float(net_str)
                                if currency == 'USDT':
                                    financial_data['net_usdt'] = net
                                elif currency == 'RUB':
                                    financial_data['net_rub'] = net
                                logger.info(f"Чистыми {currency}: {net}")
                            except (ValueError, IndexError) as e:
                                logger.warning(f"Ошибка парсинга чистых: {e}, значение: {row[13]}")
                        
                        # Комиссии по сейлзам (колонки O, P, Q, R - индексы 14, 15, 16, 17)
                        # Дима (колонка O, индекс 14)
                        if len(row) > 14 and row[14]:
                            try:
                                original_value = row[14]
                                commission_str = row[14].replace(' ', '').replace('\xa0', '').replace('₽', '').replace(',', '.')
                                commission = float(commission_str)
                                if currency == 'USDT':
                                    financial_data['dima_commission_usdt'] = commission
                                elif currency == 'RUB':
                                    financial_data['dima_commission_rub'] = commission
                                logger.info(f"Комиссия Димы {currency}: исходное='{original_value}', обработанное='{commission_str}', результат={commission}")
                            except (ValueError, IndexError) as e:
                                logger.warning(f"Ошибка парсинга комиссии Димы: {e}, значение: {row[14]}")
                        
                        # Алина (колонка P, индекс 15)
                        if len(row) > 15 and row[15]:
                            try:
                                commission_str = row[15].replace(' ', '').replace('\xa0', '').replace('₽', '').replace(',', '.')
                                commission = float(commission_str)
                                if currency == 'USDT':
                                    financial_data['alina_commission_usdt'] = commission
                                elif currency == 'RUB':
                                    financial_data['alina_commission_rub'] = commission
                                logger.info(f"Комиссия Алины {currency}: {commission}")
                            except (ValueError, IndexError) as e:
                                logger.warning(f"Ошибка парсинга комиссии Алины: {e}, значение: {row[15]}")
                        
                        # Ксения (колонка Q, индекс 16)
                        if len(row) > 16 and row[16]:
                            try:
                                original_value = row[16]
                                commission_str = row[16].replace(' ', '').replace('\xa0', '').replace('₽', '').replace(',', '.')
                                commission = float(commission_str)
                                if currency == 'USDT':
                                    financial_data['ksenia_commission_usdt'] = commission
                                elif currency == 'RUB':
                                    financial_data['ksenia_commission_rub'] = commission
                                logger.info(f"Комиссия Ксении {currency}: исходное='{original_value}', обработанное='{commission_str}', результат={commission}")
                            except (ValueError, IndexError) as e:
                                logger.warning(f"Ошибка парсинга комиссии Ксении: {e}, значение: {row[16]}")
                        
                        # Роман (колонка R, индекс 17)
                        if len(row) > 17 and row[17]:
                            try:
                                commission_str = row[17].replace(' ', '').replace('\xa0', '').replace('₽', '').replace(',', '.')
                                commission = float(commission_str)
                                if currency == 'USDT':
                                    financial_data['roman_commission_usdt'] = commission
                                elif currency == 'RUB':
                                    financial_data['roman_commission_rub'] = commission
                                logger.info(f"Комиссия Романа {currency}: {commission}")
                            except (ValueError, IndexError) as e:
                                logger.warning(f"Ошибка парсинга комиссии Романа: {e}, значение: {row[17]}")
                        
                        # Счетчики по типам оплаты (колонки S, T, U, V - индексы 18, 19, 20, 21)
                        if len(row) > 18 and row[18]:  # СБП
                            try:
                                financial_data['sbp_count'] = int(row[18])
                                logger.info(f"СБП: {row[18]}")
                            except (ValueError, IndexError):
                                pass
                        
                        if len(row) > 19 and row[19]:  # Карта
                            try:
                                financial_data['card_count'] = int(row[19])
                                logger.info(f"Карта: {row[19]}")
                            except (ValueError, IndexError):
                                pass
                        
                        if len(row) > 20 and row[20]:  # Крипта
                            try:
                                financial_data['crypto_count'] = int(row[20])
                                logger.info(f"Крипта: {row[20]}")
                            except (ValueError, IndexError):
                                pass
                        
                        if len(row) > 21 and row[21]:  # ИП
                            try:
                                financial_data['ip_count'] = int(row[21])
                                logger.info(f"ИП: {row[21]}")
                            except (ValueError, IndexError):
                                pass
            
            # Дополнительно читаем суммарные значения из ячеек M4 и N4
            try:
                # Читаем ячейку M4 (суммарная выручка)
                m4_value = sheet.acell('M4').value
                if m4_value:
                    try:
                        m4_clean = str(m4_value).replace(' ', '').replace('\xa0', '').replace('₽', '').replace(',', '.')
                        financial_data['total_revenue'] = float(m4_clean)
                        logger.info(f"Суммарная выручка из M4: {financial_data['total_revenue']}")
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Ошибка парсинга суммарной выручки из M4: {e}, значение: {m4_value}")
                
                # Читаем ячейку N4 (суммарная прибыль)
                n4_value = sheet.acell('N4').value
                if n4_value:
                    try:
                        n4_clean = str(n4_value).replace(' ', '').replace('\xa0', '').replace('₽', '').replace(',', '.')
                        financial_data['total_profit'] = float(n4_clean)
                        logger.info(f"Суммарная прибыль из N4: {financial_data['total_profit']}")
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Ошибка парсинга суммарной прибыли из N4: {e}, значение: {n4_value}")
                        
            except Exception as e:
                logger.warning(f"Не удалось прочитать ячейки M4/N4: {e}")
            
            logger.info(f"Итоговые данные: {financial_data}")
            return financial_data
            
        except Exception as e:
            logger.error(f"Ошибка получения финансовых данных: {e}")
            return {}
    
    def _handle_debug(self, message):
        """Отладочная команда для просмотра структуры таблицы"""
        try:
            if not self.sheet:
                self._init_sheets()
            
            all_values = self.sheet.get_all_values()
            
            debug_text = f"🔍 <b>Отладка таблицы</b>\n\n"
            debug_text += f"📊 Всего строк: {len(all_values)}\n\n"
            
            # Показываем первые 5 строк
            for i, row in enumerate(all_values[:5]):
                debug_text += f"<b>Строка {i}:</b>\n"
                for j, cell in enumerate(row):
                    if cell:  # Показываем только непустые ячейки
                        debug_text += f"  {chr(65+j)}{i+1}: {cell}\n"
                debug_text += "\n"
            
            # Ищем строки с валютами
            currency_rows = []
            for i, row in enumerate(all_values):
                if len(row) > 11 and row[11] in ['USDT', 'RUB']:
                    currency_rows.append(f"Строка {i}: {row[11]} - {row[12] if len(row) > 12 else 'нет данных'}")
            
            if currency_rows:
                debug_text += f"<b>Строки с валютами:</b>\n"
                for row_info in currency_rows:
                    debug_text += f"• {row_info}\n"
            else:
                debug_text += "<b>Строки с валютами не найдены</b>\n"
            
            self.bot.send_message(
                message.chat.id,
                debug_text,
                parse_mode='HTML'
            )
            
        except Exception as e:
            logger.error(f"Ошибка отладки: {e}")
            self.bot.send_message(
                message.chat.id,
                f"❌ Ошибка отладки: {str(e)}",
                parse_mode='HTML'
            )
    
    def _parse_sales_message(self, text: str) -> Optional[Dict]:
        """Парсинг сообщения о продаже"""
        # Паттерны для различных форматов
        patterns = [
            # @ads_busine 17.09 17:00 148usdt криптовалюта 1/24 внутренняя русский бизнес
            r'@(\w+)\s+(\d{1,2}\.\d{1,2})\s+(\d{1,2}:\d{2})\s+(\d+(?:\.\d+)?)(usdt|р|руб|\$|₽|юсдт)\s+(\w+)\s+(\d+/\d+)\s+(\w+)\s+(.+)',
            # Максим Шариков 12.06 1215 500р сбп 1/48 внешка русский бизнес / комментарий
            r'(\w+\s+\w+)\s+(\d{1,2}\.\d{1,2})\s+(\d{1,2}:\d{2}|\d{3,4})\s+(\d+(?:\.\d+)?)(usdt|р|руб|\$|₽|юсдт)\s+(\w+)\s+(\d+/\d+)\s+(\w+)\s+(.+)',
            # Максим Шариков 12.06 1215 500р крипта внешка 1/48 русский бизнес / комментарий
            r'(\w+\s+\w+)\s+(\d{1,2}\.\d{1,2})\s+(\d{1,2}:\d{2}|\d{3,4})\s+(\d+(?:\.\d+)?)(usdt|р|руб|\$|₽|юсдт)\s+(\w+)\s+(\w+)\s+(\d+/\d+)\s+(.+)',
            # Тарас Лобков 12 декабря 11:11 1489usdt 1/24 BusinessChannel (без @, месяц словом)
            r'(\w+\s+\w+)\s+(\d{1,2}\s+\w+)\s+(\d{1,2}:\d{2})\s+(\d+(?:\.\d+)?)(usdt|р|руб|\$|₽|юсдт)\s+(\d+/\d+)\s+(.+)',
            # Тарас Лобков 25.06 11:11 1489usdt 1/24 BusinessChannel (без @, дата с точкой)
            r'(\w+\s+\w+)\s+(\d{1,2}\.\d{1,2})\s+(\d{1,2}:\d{2})\s+(\d+(?:\.\d+)?)(usdt|р|руб|\$|₽|юсдт)\s+(\d+/\d+)\s+(.+)',
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
                had_at_prefix = text.strip().startswith('@')
                
                # Парсинг в зависимости от количества групп
                groups = match.groups()
                
                if len(groups) == 9:
                    # Проверяем, есть ли @ в начале (новый формат с @)
                    if had_at_prefix:
                        # @ads_busine 17.09 17:00 148usdt криптовалюта 1/24 внутренняя русский бизнес
                        date_str = groups[1]
                        time_str = groups[2]
                        amount = float(groups[3])
                        currency = groups[4].lower()
                        payment_type = groups[5]
                        format_str = groups[6]
                        internal_external = groups[7]
                        channel = groups[8].strip()
                        channel, comment = self._split_channel_and_comment(channel)
                    else:
                        # Новый формат: Максим Шариков 12.06 1215 500р сбп 1/48 внешка русский бизнес / комментарий
                        date_str = groups[1]
                        time_str = groups[2]
                        amount = float(groups[3])
                        currency = groups[4].lower()
                        payment_type = groups[5]
                        format_str = groups[6]
                        internal_external = groups[7]
                        channel = groups[8].strip()
                        channel, comment = self._split_channel_and_comment(channel)
                elif len(groups) == 9 and ' ' in manager:
                    # Формат: Максим Шариков 12.06 1215 500р крипта внешка 1/48 русский бизнес / комментарий
                    date_str = groups[1]
                    time_str = groups[2]
                    amount = float(groups[3])
                    currency = groups[4].lower()
                    payment_type = groups[5]
                    internal_external = groups[6]
                    format_str = groups[7]
                    channel = groups[8].strip()
                    channel, comment = self._split_channel_and_comment(channel)
                elif len(groups) == 7 and groups[5] and '/' in str(groups[5]):
                    # Старый формат с форматом (7 групп)
                    if ' ' in manager:
                        # У нас два возможных порядка: [time, date] или [date, time]
                        g2 = groups[1]
                        g3 = groups[2]
                        if ':' in g2 and ':' not in g3:
                            time_str = g2
                            date_str = g3
                        elif ':' in g3 and ':' not in g2:
                            time_str = g3
                            date_str = g2
                        else:
                            date_str = g2
                            time_str = g3
                    else:
                        date_str = groups[1]
                        time_str = groups[2]
                    amount = float(groups[3])
                    currency = groups[4].lower()
                    format_str = groups[5]
                    channel = groups[6].strip()
                    channel, comment = self._split_channel_and_comment(channel)
                    payment_type = ""
                    internal_external = ""
                elif len(groups) == 6:
                    # Формат без формата (6 групп)
                    date_str = groups[1]
                    time_str = groups[2]
                    amount = float(groups[3])
                    currency = groups[4].lower()
                    format_str = ""
                    channel = groups[5].strip()
                    channel, comment = self._split_channel_and_comment(channel)
                    payment_type = ""
                    internal_external = ""
                else:
                    # Новый формат без @ (имя фамилия ...)
                    g2 = groups[1]
                    g3 = groups[2]
                    if ':' in g2 and ':' not in g3:
                        time_str = g2
                        date_str = g3
                    elif ':' in g3 and ':' not in g2:
                        time_str = g3
                        date_str = g2
                    else:
                        date_str = g2
                        time_str = g3
                    amount = float(groups[3])
                    currency = groups[4].lower()
                    format_str = groups[5]
                    channel = groups[6].strip()
                    channel, comment = self._split_channel_and_comment(channel)
                    payment_type = ""
                    internal_external = ""
                
                # Добавляем @ только если он был в исходном сообщении
                if had_at_prefix and not manager.startswith('@'):
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
                
                # Нормализация типа оплаты
                payment_type = self._normalize_payment_type(payment_type)
                
                # Нормализация внешняя/внутренняя
                internal_external = self._normalize_internal_external(internal_external)
                
                # Парсинг даты
                try:
                    # Проверяем, что date_str не содержит время (двоеточие)
                    if ':' in date_str:
                        logger.error(f"Ошибка: date_str содержит время: {date_str}")
                        continue
                    
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
                        if len(parts) < 2:
                            logger.error(f"Ошибка: некорректный формат даты: {date_str}")
                            continue
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
                        'payment_type': payment_type,
                        'format': format_str,
                        'internal_external': internal_external,
                        'channel': channel,
                        'comment': comment
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
            # Покупатель, Дата, Время, Сумма, Валюта, Тип оплаты, Формат, Внешняя/Внутренняя, Канал где была публикация, Комментарий
            row = [
                str(data['manager']).strip(),  # Покупатель (без @, так как @ добавляется в парсере)
                data['date'],  # Дата как строка без лишних символов
                data['time'],  # Время как строка без лишних символов
                float(amount_str),  # Сумма как число
                str(data['currency']).strip(),  # Валюта
                str(data.get('payment_type', '')).strip(),  # Тип оплаты
                str(data.get('format', '')).strip(),  # Формат (может быть пустым)
                str(data.get('internal_external', '')).strip(),  # Внешняя/Внутренняя
                str(data['channel']).strip(),  # Канал
                str(data.get('comment', '')).strip()  # Комментарий
            ]
            
            # На всякий случай каждый раз убеждаемся, что используем именно вкладку 'Декабрь'
            if hasattr(self, 'spreadsheet') and self.spreadsheet:
                try:
                    self.sheet = self._ensure_december_sheet(self.spreadsheet)
                except Exception as e:
                    logger.warning(f"Не удалось переутвердить лист 'Декабрь' перед записью: {e}")

            if self.sheet:
                # Получаем следующую пустую строку
                next_row = len(self.sheet.get_all_values()) + 1
                logger.info(f"Добавляем данные в строку {next_row} Google Sheets")
                
                # Добавляем данные с явным указанием типов
                self.sheet.update(f'A{next_row}:J{next_row}', [row], value_input_option='USER_ENTERED')
                logger.info(f"✅ Данные успешно добавлены в Google Sheets: {data}")
            else:
                logger.warning(f"❌ Google Sheets не подключен! Данные записаны в режиме симуляции: {data}")
                logger.info(f"Строка для Google Sheets: {row}")
            
        except Exception as e:
            logger.error(f"Ошибка добавления в Google Sheets: {e}")
            raise
    
    def _format_amount(self, amount: float) -> str:
        """Форматирование суммы без пробелов"""
        if amount.is_integer():
            # Для целых чисел без пробелов
            return str(int(amount))
        else:
            # Для дробных чисел
            return str(amount)
    
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
✅ <b>Данные занесены в учет менеджером @{message.from_user.username}</b>

👤 <b>Покупатель:</b> {parsed_data['manager']}
📅 <b>Дата:</b> {parsed_data['date']}
🕐 <b>Время:</b> {parsed_data['time']}
💰 <b>Сумма:</b> {parsed_data['amount']} {parsed_data['currency']}
💳 <b>Тип оплаты:</b> {parsed_data.get('payment_type', 'Не указан')}
📋 <b>Формат:</b> {parsed_data.get('format', 'Не указан')}
🏢 <b>Внешняя/Внутренняя:</b> {parsed_data.get('internal_external', 'Не указано')}
📺 <b>Канал:</b> {parsed_data['channel']}
💬 <b>Комментарий:</b> {parsed_data.get('comment', 'Нет')}
                """
                
                self.bot.send_message(
                    message.chat.id,
                    confirmation_text,
                    parse_mode='HTML',
                    reply_markup=keyboard
                )
                
                # Отправляем уведомление в другой чат
                parsed_data['manager_username'] = message.from_user.username
                self._send_notification(parsed_data)
                
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
        
        # Удаляем webhook и очищаем обновления
        try:
            self.bot.remove_webhook()
            import time
            time.sleep(3)
            # Очищаем очередь обновлений
            self.bot.get_updates(offset=-1)
            time.sleep(2)
        except Exception as e:
            logger.warning(f"Ошибка при очистке: {e}")
        
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.bot.polling(none_stop=True, interval=0, timeout=20)
                break
            except Exception as e:
                retry_count += 1
                logger.error(f"Ошибка запуска бота (попытка {retry_count}/{max_retries}): {e}")
                
                if "409" in str(e) or "Conflict" in str(e):
                    logger.info("Обнаружен конфликт 409, пытаемся снять webhook и очистить обновления...")
                    try:
                        self.bot.remove_webhook()
                        import time
                        time.sleep(5)
                        # Очищаем очередь обновлений
                        self.bot.get_updates(offset=-1)
                        time.sleep(5)
                    except Exception as webhook_error:
                        logger.warning(f"Не удалось снять webhook: {webhook_error}")
                
                if retry_count < max_retries:
                    import time
                    time.sleep(10)  # Увеличиваем время ожидания
                else:
                    logger.error("Достигнуто максимальное количество попыток запуска")
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
