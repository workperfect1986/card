import os
import time
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import uvicorn

app = FastAPI()

# Estado simples
class Estado:
    def __init__(self):
        self.rodando = False
        self.clients = set()

estado = Estado()

# WebSocket
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    estado.clients.add(websocket)
    try:
        await websocket.send_json({"type": "status", "rodando": estado.rodando})
        while True:
            data = await websocket.receive_json()
    except WebSocketDisconnect:
        pass
    finally:
        estado.clients.discard(websocket)

# Rota principal com HTML inline
@app.get("/", response_class=HTMLResponse)
async def index():
    return """
    <!DOCTYPE html>
    <html>
    <head><title>Teste WebSocket</title></head>
    <body style="background:#111;color:#fff;font-family:sans-serif;padding:20px">
        <h1>🚀 WebSocket Test</h1>
        <button onclick="testar()">Enviar Ping</button>
        <div id="log"></div>
        <script>
            const ws = new WebSocket(`ws://${location.host}/ws`);
            ws.onmessage = (e) => {
                document.getElementById('log').innerHTML += '<p>' + JSON.stringify(JSON.parse(e.data)) + '</p>';
            };
            function testar() {
                ws.send(JSON.stringify({type: "ping"}));
            }
        </script>
    </body>
    </html>"""

@app.get("/api/health")
async def health():
    return {"status": "ok"}

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    print(f"Iniciando em 0.0.0.0:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
