# 🔧 Переменные окружения для Railway

Скопируйте эти переменные в Railway Dashboard → Variables:

```
TELEGRAM_BOT_TOKEN=8361266417:AAEfwm_4kJHnLopUyH_sA3nArNcb42CcRpQ
GOOGLE_SHEETS_ID=1KGi1sDNqFzSZwDJLa9zcCXAv6fwbyOmEF-34eZdQKXc
CREDENTIALS_FILE=credentials.json
CREDENTIALS_FOLDER=credentials
NOTIFICATION_CHAT_ID=-1001234567890
```

## 📝 Инструкция:

1. В Railway Dashboard перейдите в раздел "Variables"
2. Добавьте каждую переменную отдельно
3. Убедитесь, что `credentials.json` загружен в Files
4. Для получения ID чата:
   - Добавьте бота в нужный чат
   - Напишите боту любое сообщение
   - Посмотрите в логах ID чата
   - Замените `-1001234567890` на реальный ID
