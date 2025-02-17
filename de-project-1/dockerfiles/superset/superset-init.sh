#!/bin/bash

# Инициализация базы данных Superset
superset db upgrade

# Создание учетной записи администратора
superset fab create-admin --username "${ADMIN_USERNAME}" --firstname Superset --lastname Admin --email "${ADMIN_EMAIL}" --password "${ADMIN_PASSWORD}"

# Загрузка примерных данных (если необходимо)
# superset load_examples

# Настройка Superset
superset init

# Запуск Superset
superset run -p 8088 -h 0.0.0.0 --with-threads --reload --debugger

# Ждем 60 секунд перед добавлением подключения к базе данных
# sleep 60

# # Добавление подключения к базе данных PostgreSQL
superset dbs add \
  --database-name dwh \
  --sqlalchemy-uri postgresql+psycopg2://de:engineer@dwh_pg:5432/dwh \
  --configuration-method sql \
  --username admin \
  --password admin
