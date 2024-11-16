# Базовый образ
FROM python:3.12

# Отключаем буферизацию и записывание байт-кода
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Устанавливаем зависимости для Python и системы, включая PostgreSQL 17
RUN apt-get update && apt-get install -y \
    wget \
    gnupg2 \
    lsb-release

# Добавляем репозиторий PostgreSQL 17 и устанавливаем его
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    git \
    gettext \
    netcat-openbsd \
    wkhtmltopdf \
    fonts-noto-color-emoji \
    cron \
    nginx \
    supervisor \
    postgresql-17 postgresql-client-17 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Устанавливаем clickhouse-backup
RUN wget https://github.com/Altinity/clickhouse-backup/releases/download/v2.6.3/clickhouse-backup-linux-amd64.tar.gz \
    && tar -zxvf clickhouse-backup-linux-amd64.tar.gz \
    && mv build/linux/amd64/clickhouse-backup /usr/local/bin/clickhouse-backup \
    && rm -f clickhouse-backup-linux-amd64.tar.gz \
    && mkdir -p /etc/clickhouse-backup

# Устанавливаем рабочую директорию
ENV HOME=/backup_manager/
WORKDIR $HOME

# Копируем requirements и устанавливаем зависимости
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Копируем и настраиваем скрипт запуска
COPY ./compose/local/django/start /start
RUN sed -i 's/\r$//g' /start && chmod +x /start

# Копируем cron файл и настраиваем его
COPY ./compose/local/django/cronfile /etc/cron.d/dump_cron
RUN chmod 0644 /etc/cron.d/dump_cron && crontab /etc/cron.d/dump_cron

# Копируем все файлы приложения
COPY ./apps .

# Настройка логов и директорий
RUN mkdir -p /backup_manager/logs
RUN touch /var/log/cron.log

# Копируем конфигурацию nginx
COPY compose/local/nginx/nginx.conf /etc/nginx/conf.d/default.conf

# Копируем конфигурацию для supervisord
COPY compose/local/django/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Экспонируем порты
EXPOSE 80 8009

# Запуск supervisord
CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]