# pull official base image
FROM python:3.12
ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

RUN apt-get update \
  # dependencies for building Python packages
  && apt-get install -y build-essential \
  # psycopg2 dependencies
  && apt-get install -y libpq-dev \
  # git
  && apt-get install -y git \
  && apt-get install -y gettext \
  && apt-get install -y netcat-openbsd \
  && apt-get install -y wkhtmltopdf \
  && apt-get install -y fonts-noto-color-emoji \
  # pygraphviz
#  && apt-get install -y python-pygraphviz libgraphviz-dev \
  # cleaning up unused files
  && apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false \
  && rm -rf /var/lib/apt/lists/*

ENV HOME /backup_manager/

# set work directory
WORKDIR $HOME

# Requirements are installed here to ensure they will be cached.
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# copy entrypoint
COPY ./compose/local/django/entrypoint /entrypoint
RUN sed -i 's/\r$//g' /entrypoint
RUN chmod +x /entrypoint

COPY ./compose/local/django/start /start
RUN sed -i 's/\r$//g' /start
RUN chmod +x /start

RUN mkdir -p /backup_manager/logs

COPY ./apps .
