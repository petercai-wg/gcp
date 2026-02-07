#!/usr/bin/env bash
python manage.py collectstatic --noinput
python manage.py migrate --noinput

# Create superuser if environment variables are set
if [ "$DJANGO_SUPERUSER_USERNAME" ] && [ "$DJANGO_SUPERUSER_PASSWORD" ]; then
  python manage.py createsuperuser --noinput --username "$DJANGO_SUPERUSER_USERNAME" --email "$DJANGO_SUPERUSER_EMAIL"
fi

python -m gunicorn --bind 0.0.0.0:8080 --workers 2 mywebsite.wsgi:application