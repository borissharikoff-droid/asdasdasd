# 🔧 Переменные окружения для Railway

Скопируйте эти переменные в Railway Dashboard → Variables:

```
TELEGRAM_BOT_TOKEN=<your_telegram_bot_token>
GOOGLE_SHEETS_ID=<your_google_sheet_id>
CREDENTIALS_FILE=credentials.json
CREDENTIALS_FOLDER=credentials
NOTIFICATION_CHAT_ID=-1001234567890#123   # замените на свой
```

## 📝 Инструкция:

1. В Railway Dashboard перейдите в раздел "Variables"
2. Добавьте каждую переменную отдельно
3. Убедитесь, что `credentials.json` загружен в Files
4. Для получения ID чата/топика:
   - Добавьте бота в нужный чат
   - Напишите боту в нужном топике любое сообщение
   - Посмотрите в логах ID чата и топика
   - Замените `-1001234567890#123` на реальный ID
   - Формат: `ID_ЧАТА#ID_ТОПИКА`
