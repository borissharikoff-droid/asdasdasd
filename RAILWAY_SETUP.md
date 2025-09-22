# 🚀 Настройка Railway для Sales Bot

## 📋 Обязательные переменные окружения

Добавьте в Railway Dashboard → Variables:

```
TELEGRAM_BOT_TOKEN=8361266417:AAGtg7botE6HqAb92nUKDV_iGD7-S0LViuY
GOOGLE_SHEETS_ID=1KGi1sDNqFzSZwDJLa9zcCXAv6fwbyOmEF-34eZdQKXc
CREDENTIALS_FILE=credentials.json
CREDENTIALS_FOLDER=credentials
LOG_LEVEL=INFO
```

## 📁 Загрузка файлов

### 1. Загрузите credentials.json
- В Railway Dashboard → **Files**
- Загрузите файл `credentials.json` в корень проекта
- Или создайте папку `credentials` и загрузите туда

### 2. Проверьте доступ к Google Sheets
- Откройте таблицу: https://docs.google.com/spreadsheets/d/1KGi1sDNqFzSZwDJLa9zcCXAv6fwbyOmEF-34eZdQKXc/edit
- Поделитесь таблицей с email: `telegram-bot-sheets-sales-trac@extended-cache-467123-u2.iam.gserviceaccount.com`
- Дайте права **Редактор**

## 🔍 Диагностика проблем

### Проверьте логи Railway:
1. Откройте Railway Dashboard → **Deployments**
2. Нажмите на последний деплой
3. Перейдите в **Logs**

### Ожидаемые сообщения в логах:
```
Google Sheets ID из конфига: 1KGi1sDNqFzSZwDJLa9zcCXAv6fwbyOmEF-34eZdQKXc
Открываем Google Sheets с ID: 1KGi1sDNqFzSZwDJLa9zcCXAv6fwbyOmEF-34eZdQKXc
Успешно подключились к Google Sheets: [название таблицы]
Google Sheets подключен успешно
```

### Если видите ошибки:
- ❌ `Google Sheets не подключен!` - проверьте credentials.json
- ❌ `Не удалось подключиться к Google Sheets` - проверьте доступ к таблице
- ❌ `TELEGRAM_BOT_TOKEN не найден` - проверьте переменные окружения

## 🧪 Тестирование

После настройки отправьте боту сообщение в формате:
```
@testuser 12.04 1500 500р 1/24 "Тест"
```

В логах должно появиться:
```
✅ Данные успешно добавлены в Google Sheets: {...}
```

## 📊 Структура Google Sheets

Бот автоматически создаст заголовки:
- Покупатель
- Дата  
- Время
- Сумма
- Валюта
- Тип оплаты
- Формат
- Внешняя/Внутренняя
- Канал где была публикация
- Комментарий
