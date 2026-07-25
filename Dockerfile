FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    wget gnupg ca-certificates \
    fonts-liberation fonts-unifont \
    libasound2 libatk-bridge2.0-0 libatk1.0-0 \
    libcups2 libdbus-1-3 libdrm2 libgbm1 \
    libgtk-3-0 libnspr4 libnss3 \
    libwayland-client0 libxcomposite1 \
    libxdamage1 libxfixes3 libxkbcommon0 \
    libxrandr2 xdg-utils \
    && rm -rf /var/lib/apt/lists/*

RUN pip install playwright fastapi uvicorn pydantic websockets && \
    playwright install-deps chromium && \
    playwright install chromium

WORKDIR /app
COPY . .
RUN mkdir -p templates

EXPOSE 8000
CMD ["python", "main.py"]
