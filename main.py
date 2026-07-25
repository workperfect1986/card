import os
import sys
import time
import asyncio
import subprocess
from pathlib import Path
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import uvicorn

# ================= CONFIGURAÇÕES =================
PORT = int(os.getenv("PORT", "8000"))

# ================= INSTALAÇÃO DO PLAYWRIGHT (se necessário) =================
def garantir_playwright():
    """Verifica e instala o Chromium se não existir."""
    chromium_path = Path("/root/.cache/ms-playwright/chromium-*/chrome-linux/chrome")
    instalado = any(chromium_path.parent.parent.exists() for chromium_path in [Path(p) for p in ["/root/.cache/ms-playwright/chromium-1097/chrome-linux/chrome"]])
    
    # Verifica se existe alguma pasta de Chromium
    ms_playwright = Path("/root/.cache/ms-playwright")
    if ms_playwright.exists():
        for pasta in ms_playwright.iterdir():
            if pasta.is_dir() and pasta.name.startswith("chromium-"):
                chrome = pasta / "chrome-linux" / "chrome"
                if chrome.exists():
                    print(f"✅ Chromium encontrado: {chrome}")
                    return True
    
    print("⚠️ Chromium não encontrado. Instalando...")
    try:
        subprocess.run(["playwright", "install", "chromium"], check=True, timeout=120)
        print("✅ Chromium instalado com sucesso!")
        return True
    except Exception as e:
        print(f"❌ Erro ao instalar Chromium: {e}")
        return False

# Executar verificação no startup
PLAYWRIGHT_OK = garantir_playwright()

# ================= ESTADO GLOBAL =================
class Estado:
    def __init__(self):
        self.rodando = False
        self.clients: set = set()

estado = Estado()

# ================= APLICAÇÃO =================
app = FastAPI(title="Unimar Card Tester - Teste")

# ================= WEBSOCKET =================
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    estado.clients.add(websocket)
    print(f"🟢 Cliente conectado. Total: {len(estado.clients)}")
    try:
        await websocket.send_json({
            "type": "status",
            "rodando": estado.rodando,
            "clientes": len(estado.clients),
            "playwright_ok": PLAYWRIGHT_OK
        })
        while True:
            data = await websocket.receive_json()
            if data.get("type") == "ping":
                await websocket.send_json({"type": "pong"})
    except WebSocketDisconnect:
        print("🔴 Cliente desconectado")
    except Exception as e:
        print(f"Erro WebSocket: {e}")
    finally:
        estado.clients.discard(websocket)

# ================= ROTAS =================
@app.get("/", response_class=HTMLResponse)
async def index():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Teste WebSocket + Playwright</title>
        <meta charset="UTF-8">
        <style>
            body { background:#0a0a0a; color:#fff; font-family:sans-serif; padding:20px; }
            button { padding:10px; margin:5px; }
            #log { background:#1a1a1a; padding:10px; margin-top:20px; height:200px; overflow-y:auto; font-family:monospace; font-size:12px; }
        </style>
    </head>
    <body>
        <h1>🚀 Teste WebSocket + Playwright</h1>
        <p>Status Playwright: <span id="status">...</span></p>
        <button onclick="ping()">📡 Ping WebSocket</button>
        <button onclick="testarPlaywright()">🧪 Testar Playwright</button>
        <div id="log"></div>
        <script>
            const ws = new WebSocket(`ws://${location.host}/ws`);
            ws.onopen = () => log('✅ WebSocket conectado');
            ws.onmessage = (e) => {
                const data = JSON.parse(e.data);
                log('📩 ' + JSON.stringify(data));
                if (data.playwright_ok !== undefined) {
                    document.getElementById('status').textContent = data.playwright_ok ? '✅ Instalado' : '❌ Não instalado';
                }
            };
            ws.onclose = () => log('❌ WebSocket desconectado');

            function log(msg) {
                document.getElementById('log').innerHTML += `<div>${msg}</div>`;
            }
            function ping() {
                ws.send(JSON.stringify({type: 'ping'}));
            }
            async function testarPlaywright() {
                const resp = await fetch('/api/playwright-test');
                const data = await resp.json();
                log('🧪 Playwright: ' + JSON.stringify(data));
            }
        </script>
    </body>
    </html>"""

@app.get("/api/health")
async def health():
    return {"status": "ok", "timestamp": time.time()}

@app.get("/api/playwright-test")
async def playwright_test():
    try:
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage"]
            )
            page = await browser.new_page()
            await page.goto("https://httpbin.org/get", timeout=15000)
            content = await page.content()
            await browser.close()
            return {"status": "ok", "tamanho_pagina": len(content)}
    except Exception as e:
        return {"status": "erro", "mensagem": str(e)}

# ================= INICIALIZAÇÃO =================
if __name__ == "__main__":
    print("=" * 50)
    print(f"🚀 Iniciando servidor em 0.0.0.0:{PORT}")
    print(f"🔗 Health: http://0.0.0.0:{PORT}/api/health")
    print(f"📡 WebSocket: ws://0.0.0.0:{PORT}/ws")
    print(f"🎭 Playwright instalado: {PLAYWRIGHT_OK}")
    print("=" * 50)
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
