import os
import sys
import time
from fastapi import FastAPI
import uvicorn

app = FastAPI()

@app.get("/")
async def root():
    return {
        "message": "Unimar Card Tester ONLINE",
        "time": time.strftime("%H:%M:%S"),
        "port": os.getenv("PORT", "não definida")
    }

@app.get("/api/health")
async def health():
    return {"status": "ok"}

if __name__ == "__main__":
    PORT = int(os.getenv("PORT", "8000"))
    
    print("=" * 50, flush=True)
    print(f"Iniciando servidor em 0.0.0.0:{PORT}", flush=True)
    print(f"Health check: http://0.0.0.0:{PORT}/api/health", flush=True)
    print("=" * 50, flush=True)
    
    # Iniciar o servidor com configurações explícitas para Railway
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=PORT,
        log_level="info",
        access_log=True
    )
