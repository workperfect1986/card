FROM python:3.11-slim

# Dependências de sistema para o Chromium (Debian Trixie compatível)
RUN apt-get update && apt-get install -y \
    wget gnupg ca-certificates curl \
    fonts-liberation fonts-unifont \
    libasound2 libatk-bridge2.0-0 libatk1.0-0 \
    libcups2 libdbus-1-3 libdrm2 libgbm1 libglib2.0-0 \
    libgtk-3-0 libnspr4 libnss3 libxss1 \
    libwayland-client0 libxcomposite1 \
    libxdamage1 libxfixes3 libxkbcommon0 \
    libxrandr2 xdg-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .

# Instala pacotes Python e depois o Chromium SEM --with-deps
# (deps já instaladas manualmente acima, evita erro com ttf-unifont no Debian Trixie)
RUN pip install --no-cache-dir -r requirements.txt \
    && playwright install chromium

COPY . .
EXPOSE 8000
CMD ["python", "main.py"]
