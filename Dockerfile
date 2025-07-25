FROM python:3.12-slim

# ДОБАВЬТЕ ЭТУ СТРОКУ
RUN apt-get update && apt-get install -y ffmpeg

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PYTHONUNBUFFERED=1
CMD ["python", "support_bot.py"]