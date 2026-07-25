FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    wget gnupg ca-certificates \
    fonts-liberation fonts-unifont \
    libasound2 libatk-bridge2.0-0 libatk1.0-0 \
    libcups2 libdbus-1-3 libdrm2 libgbm1 \
    libgtk-3-0 libnspr4 libnss3 \
    libwayland-client0 libxcomposite1 \
    libxdamage1 libxfixes3 libxkbcommon0 \
    libxrandr2 xdg-utils curl \
    && rm -rf /var/lib/apt/lists/*

# Instalar Playwright PRIMEIRO (mais pesado)
RUN pip install playwright && \
    playwright install-deps chromium && \
    playwright install chromium

# Definir diretório de trabalho
WORKDIR /app

# Copiar requirements
COPY requirements.txt .

# Instalar outras dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copiar TODO o código
COPY . .

# Verificar estrutura de arquivos
RUN echo "=== ESTRUTURA DE ARQUIVOS ===" && \
    ls -la && \
    echo "=== TEMPLATES ===" && \
    ls -la templates/ 2>/dev/null || echo "Pasta templates não encontrada" && \
    mkdir -p templates

# Expor porta
EXPOSE 8000

# Iniciar
CMD ["python", "main.py"]
