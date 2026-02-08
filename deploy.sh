#!/bin/sh

time gcloud run deploy pyas2-habits \
  --source . \
  --set-secrets="MONGO_URL=mongo-url-gaq:latest,PYASSISTANTBOT_MONGO_URL=mongo-url-s8:latest,TELEGRAM_TOKEN=20250628-telegram-token-alex-gemini-bot:latest" \
  --set-env-vars="CHAT_ID=$CHAT_ID" \
  --timeout=600 \
  --region us-east1 --no-allow-unauthenticated;
