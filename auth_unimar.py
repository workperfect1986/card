import os
import json
import time
import logging
from pathlib import Path
from urllib.parse import urlparse
import httpx
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("unimar.auth")

URL_LOGIN = os.getenv("UNIMAR_URL_LOGIN", "https://digital.unimar.br/login/app")
URL_MEUS_CARTOES = os.getenv(
    "UNIMAR_URL_CARTOES", "https://digital.unimar.br/areadoaluno/conta/meuscartoes"
)
EMAIL = os.getenv("UNIMAR_EMAIL", "")
SENHA = os.getenv("UNIMAR_SENHA", "")
SESSAO_PATH = Path(os.getenv("UNIMAR_SESSAO_PATH", "sessao_unimar.json"))
SESSAO_MAX_AGE = int(os.getenv("UNIMAR_SESSAO_MAX_AGE", "86400"))

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

HEADERS_HTTP = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/jxl,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "cache-control": "max-age=0",
    "content-type": "application/x-www-form-urlencoded",
    "origin": "https://digital.unimar.br",
    "referer": URL_LOGIN,
    "upgrade-insecure-requests": "1",
    "user-agent": USER_AGENT,
}


def sessao_local_expirada() -> bool:
    """Verifica se o arquivo de sessão não existe ou ultrapassou o tempo máximo."""
    if not SESSAO_PATH.exists():
        return True
    return (time.time() - SESSAO_PATH.stat().st_mtime) > SESSAO_MAX_AGE


def _cookies_para_storage_state(client: httpx.AsyncClient) -> dict:
    """Converte os cookies da sessão httpx para o formato storage_state do Playwright."""
    cookies_playwright = []
    hostname_padrao = urlparse(URL_LOGIN).hostname or "digital.unimar.br"

    for cookie in client.cookies.jar:
        domain = cookie.domain or hostname_padrao
        cookies_playwright.append(
            {
                "name": cookie.name,
                "value": cookie.value,
                "domain": domain,
                "path": cookie.path or "/",
                "expires": float(cookie.expires) if cookie.expires else -1,
                "httpOnly": bool(cookie.has_nonstandard_attr("HttpOnly")),
                "secure": bool(cookie.secure),
                "sameSite": "Lax",
            }
        )

    return {"cookies": cookies_playwright, "origins": []}


async def validar_sessao_http() -> bool:
    """Valida rapidamente via HTTP (sem abrir aba) se os cookies salvos ainda estão autenticados."""
    if sessao_local_expirada():
        return False

    try:
        dados = json.loads(SESSAO_PATH.read_text(encoding="utf-8"))
        cookies = httpx.Cookies()
        for c in dados.get("cookies", []):
            cookies.set(
                name=c["name"],
                value=c["value"],
                domain=c.get("domain", ""),
                path=c.get("path", "/"),
            )

        headers_get = {k: v for k, v in HEADERS_HTTP.items() if k != "content-type"}
        async with httpx.AsyncClient(
            headers=headers_get,
            cookies=cookies,
            follow_redirects=True,
            timeout=12.0,
        ) as client:
            resp = await client.get(URL_MEUS_CARTOES)
            if "login" in str(resp.url).lower():
                return False
            return True
    except Exception:
        logger.warning("[Auth HTTP] Erro ao validar sessão existente", exc_info=True)
        return False


async def login_via_http(log_fn=None) -> bool:
    """Realiza o POST de autenticação direto usando credenciais do .env e salva sessao_unimar.json."""
    if not EMAIL or not SENHA:
        if log_fn:
            log_fn("[ERRO] UNIMAR_EMAIL ou UNIMAR_SENHA não configurados no .env!")
        return False

    payload = {
        "login": EMAIL,
        "passwd": SENHA,
        "lembrar": "1",
        "btnAcessar": "",
    }

    try:
        async with httpx.AsyncClient(
            headers=HEADERS_HTTP,
            follow_redirects=True,
            timeout=20.0,
        ) as client:
            # 1. GET inicial para capturar cookies de sessão gerados pelo servidor
            headers_get = {k: v for k, v in HEADERS_HTTP.items() if k != "content-type"}
            await client.get(URL_LOGIN, headers=headers_get)

            # 2. POST com as credenciais do .env
            resp = await client.post(URL_LOGIN, data=payload)
            resp.raise_for_status()

            # 3. Confirma acesso à área logada
            check = await client.get(URL_MEUS_CARTOES, headers=headers_get)
            url_final = str(check.url).lower()

            if "login" in url_final:
                if log_fn:
                    log_fn("[Auth HTTP] Redirecionado para login após POST (credenciais inválidas ou bloqueio JS).")
                return False

            storage = _cookies_para_storage_state(client)
            if not storage["cookies"]:
                if log_fn:
                    log_fn("[Auth HTTP] Nenhum cookie de sessão retornado.")
                return False

            SESSAO_PATH.write_text(
                json.dumps(storage, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            return True
    except Exception as exc:
        logger.exception("[Auth HTTP] Falha na requisição de login")
        if log_fn:
            log_fn(f"[Auth HTTP] Erro: {type(exc).__name__}")
        return False


async def _login_fallback_playwright(browser, viewport: dict, log_fn=None) -> bool:
    """Fallback via navegador caso o portal exija JS/validação adicional no login."""
    context = await browser.new_context(viewport=viewport, user_agent=USER_AGENT)
    page = await context.new_page()
    page.on("dialog", lambda d: d.accept())

    try:
        if log_fn:
            log_fn("[Auth Fallback] Executando login via navegador...")

        await page.goto(URL_LOGIN, wait_until="domcontentloaded", timeout=15000)

        preenchido = await page.evaluate(
            """([email, senha]) => {
                const emailInput = document.querySelector('input[name="login"], input[type="email"], input[type="text"]');
                const passInput = document.querySelector('input[name="passwd"], input[type="password"]');
                const checkInput = document.querySelector('input[name="lembrar"], input[type="checkbox"]');
                if (!emailInput || !passInput) return false;

                emailInput.value = email;
                emailInput.dispatchEvent(new Event('input', { bubbles: true }));
                emailInput.dispatchEvent(new Event('change', { bubbles: true }));

                passInput.value = senha;
                passInput.dispatchEvent(new Event('input', { bubbles: true }));
                passInput.dispatchEvent(new Event('change', { bubbles: true }));

                if (checkInput) {
                    checkInput.checked = true;
                    checkInput.dispatchEvent(new Event('change', { bubbles: true }));
                }
                return true;
            }""",
            [EMAIL, SENHA],
        )

        if not preenchido:
            if log_fn:
                log_fn("[ERRO] Campos de login não encontrados no DOM.")
            return False

        submetido = await page.evaluate(
            """() => {
                const btn = document.querySelector('button[name="btnAcessar"], input[name="btnAcessar"], button[type="submit"], input[type="submit"]');
                if (btn) { btn.click(); return true; }
                const form = document.querySelector('form');
                if (form) { form.submit(); return true; }
                return false;
            }"""
        )

        if not submetido:
            await page.keyboard.press("Enter")

        try:
            await page.wait_for_url("**/areadoaluno/**", timeout=12000)
        except Exception:
            await page.wait_for_load_state("domcontentloaded", timeout=8000)

        if "login" in page.url.lower():
            await page.goto(URL_MEUS_CARTOES, wait_until="domcontentloaded", timeout=15000)
            if "login" in page.url.lower():
                return False

        await context.storage_state(path=str(SESSAO_PATH))
        return True
    finally:
        await context.close()


async def garantir_autenticacao(
    browser,
    viewport: dict,
    log_fn=None,
    limpar_fn=None,
    forcar_novo_login: bool = False,
) -> bool:
    """
    Orquestra a autenticação de alta performance:
    1. Valida sessão salva via HTTP (instantâneo)
    2. Se expirada, faz login direto via HTTP POST usando .env
    3. Se o HTTP falhar (ex: proteção JS), usa fallback Playwright
    4. Executa limpeza inicial de cartões se solicitada
    """
    if not EMAIL or not SENHA:
        if log_fn:
            log_fn("[ERRO] Email/senha não configurados no .env!")
        return False

    # Passo 1: Reutilizar sessão existente validando via HTTP
    if not forcar_novo_login and await validar_sessao_http():
        if log_fn:
            log_fn("[Auth] Sessão reutilizada via HTTP (Modo Ultra-Fast)")
        return True

    if SESSAO_PATH.exists():
        if log_fn:
            log_fn("[Auth] Sessão expirada, renovando via HTTP POST...")
        try:
            SESSAO_PATH.unlink()
        except Exception:
            pass
    else:
        if log_fn:
            log_fn("[Auth] Iniciando login via HTTP POST (.env)...")

    # Passo 2: Tentar login rápido via HTTP POST
    autenticado = await login_via_http(log_fn=log_fn)

    # Passo 3: Fallback via Playwright se o POST direto não bastar
    if not autenticado:
        autenticado = await _login_fallback_playwright(browser, viewport, log_fn=log_fn)

    if not autenticado:
        return False

    if log_fn:
        log_fn("[Auth] Sessão autenticada e salva com sucesso!")

    # Passo 4: Faxina inicial de cartões usando a sessão recém-criada
    if limpar_fn:
        context = await browser.new_context(
            storage_state=str(SESSAO_PATH),
            viewport=viewport,
            user_agent=USER_AGENT,
        )
        page = await context.new_page()
        page.on("dialog", lambda d: d.accept())
        try:
            await limpar_fn(page)
        finally:
            await context.close()

    return True
