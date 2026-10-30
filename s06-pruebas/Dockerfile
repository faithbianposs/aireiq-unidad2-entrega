FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY app/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

CMD ["sleep", "infinity"]