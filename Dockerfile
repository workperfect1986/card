# Crie o arquivo
cat > Dockerfile << 'EOF'
FROM python:3.11-slim

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    fonts-unifont \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libwayland-client0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

# Instalar Playwright com dependências
RUN pip install playwright && \
    playwright install chromium && \
    playwright install-deps chromium

# Configurar diretório de trabalho
WORKDIR /app

# Copiar requirements primeiro (cache do Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar resto do código
COPY . .

# Criar diretórios necessários
RUN mkdir -p templates && \
    touch aprovados.txt

# Expor porta
EXPOSE 5000

# Comando para iniciar
CMD ["python", "main.py"]
EOF

# Adicione ao git
git add Dockerfile .dockerignore
git commit -m "fix: Corrige instalação do Playwright no Railway"
git push
