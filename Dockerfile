FROM python:3.13-slim

LABEL authors="ailyaev"

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

#COPY zscaler-root-ca.crt /usr/local/share/ca-certificates/zscaler-root-ca.crt

RUN apt-get update && apt-get install -y \
    ca-certificates \
    gcc \
    default-libmysqlclient-dev \
    pkg-config \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/

RUN python manage.py collectstatic --noinput

CMD ["sh", "-c", "gunicorn shiluvim.wsgi:application --bind 0.0.0.0:${PORT:-8080}"]