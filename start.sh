#!/bin/bash
set -e

echo "========================================="
echo "🚀 Iniciando Unimar Card Tester"
echo "========================================="
echo "Python: $(python --version)"
echo "Host: 0.0.0.0"
echo "Port: ${PORT:-8000}"
echo "========================================="

# Verificar se o Playwright está instalado
python -c "from playwright.async_api import async_playwright; print('✅ Playwright OK')" || {
    echo "❌ Playwright não encontrado. Instalando..."
    pip install playwright
    playwright install chromium
    playwright install-deps chromium
}

# Iniciar servidor
exec python main.py
