# 🤖 Sales Bot для Telegram

Telegram бот для записи данных о продажах в Google Sheets.

## 🚀 Быстрый деплой в Railway

### 1. Создайте репозиторий на GitHub
```bash
git init
git add .
git commit -m "Sales Bot"
git remote add origin https://github.com/yourusername/sales-bot.git
git push -u origin main
```

### 2. Деплой в Railway
1. Перейдите на [railway.app](https://railway.app)
2. Войдите через GitHub
3. "New Project" → "Deploy from GitHub repo"
4. Выберите ваш репозиторий

### 3. Настройте переменные окружения
В Railway Dashboard → Variables добавьте (значения подставьте свои):
```
TELEGRAM_BOT_TOKEN=<your_telegram_bot_token>
GOOGLE_SHEETS_ID=<your_google_sheet_id>
CREDENTIALS_FILE=credentials.json
CREDENTIALS_FOLDER=credentials
```

### 4. Загрузите credentials.json
В Railway Dashboard → Files загрузите файл `credentials.json`

## 📝 Форматы сообщений

### С @:
```
@maxim 12.04 1719 522р 1/24 "АНУС"
@Boris 16.04 1500 322usdt 1/48 "Бизнес и Бизнес"
```

### Без @ (имя и фамилия):
```
Ксения Вантрип 1230 16.04 501юсдт 1/24 БиБ
Анна Петрова 12:30 15.05 750р 1/48 ТестКанал
```

## 📊 Google Sheets

Бот автоматически создает таблицу с колонками:
- Покупатель
- Дата
- Время
- Сумма (с пробелами для тысяч)
- Валюта
- Формат
- Канал где была публикация

## 🎯 Команды

- `/start` - главное меню
- `/stats` - статистика продаж

## 📁 Структура проекта

```
├── main.py          # Основной файл бота
├── config.py        # Конфигурация
├── requirements.txt # Зависимости
├── Procfile        # Команда запуска для Railway
├── runtime.txt     # Версия Python
├── railway.json    # Конфигурация Railway
├── credentials/    # Папка с Google API ключами
└── README.md       # Документация
```

## ✅ Готово!

Бот будет работать 24/7 в Railway! 🚀