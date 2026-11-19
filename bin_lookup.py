import os
import asyncio
import logging
import httpx
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("unimar.bin")

BIN_API_URL_TEMPLATE = os.getenv(
    "BIN_API_URL_TEMPLATE", "https://data.handyapi.com/bin/{bin}"
)
BIN_FALLBACK_VALUE = os.getenv("BIN_FALLBACK_VALUE", "N/A")

# Cache em memória por BIN (6 dígitos) durante a execução
_BIN_CACHE: dict[str, dict[str, str]] = {}
_BIN_CACHE_LOCK = asyncio.Lock()


def validar_valor_bin(valor) -> str:
    """Normaliza valores retornados pela consulta de BIN evitando vazios ou nulos."""
    if valor is None:
        return BIN_FALLBACK_VALUE
    texto = str(valor).strip()
    if not texto or texto.lower() in {"null", "none", "undefined", "n/a", "na"}:
        return BIN_FALLBACK_VALUE
    return texto


def _extrair_campo_ci(payload: dict, *chaves: str):
    """Busca chaves no dicionário ignorando diferença entre maiúsculas e minúsculas."""
    mapa_lower = {str(k).lower(): v for k, v in payload.items()}
    for chave in chaves:
        if chave in payload and payload[chave] is not None:
            return payload[chave]
        chave_lower = chave.lower()
        if chave_lower in mapa_lower and mapa_lower[chave_lower] is not None:
            return mapa_lower[chave_lower]
    return None


def _resultado_padrao() -> dict[str, str]:
    return {
        "scheme": BIN_FALLBACK_VALUE,
        "issuer": BIN_FALLBACK_VALUE,
        "type": BIN_FALLBACK_VALUE,
        "tier": BIN_FALLBACK_VALUE,
    }


def limpar_cache_bin() -> None:
    """Limpa o cache em memória de BINs (útil caso queira resetar entre execuções)."""
    _BIN_CACHE.clear()


async def consultar_bin(numero_cartao: str, usar_cache: bool = True) -> dict[str, str]:
    """
    Consulta os dados do BIN (6 primeiros dígitos) na HandyAPI com cache em memória.
    Retorna um dicionário com scheme, issuer, type e tier normalizados.
    """
    digitos = "".join(ch for ch in numero_cartao if ch.isdigit())
    if len(digitos) < 6:
        return _resultado_padrao()

    bin_code = digitos[:6]

    if usar_cache:
        async with _BIN_CACHE_LOCK:
            if bin_code in _BIN_CACHE:
                logger.debug("Cache BIN hit para %s", bin_code)
                return dict(_BIN_CACHE[bin_code])

    url = BIN_API_URL_TEMPLATE.format(bin=bin_code)
    resultado = _resultado_padrao()

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, dict):
                    scheme_raw = _extrair_campo_ci(data, "Scheme", "scheme", "Brand", "brand")
                    issuer_raw = _extrair_campo_ci(data, "Issuer", "issuer", "Bank", "bank")
                    type_raw = _extrair_campo_ci(data, "Type", "type")
                    tier_raw = _extrair_campo_ci(
                        data, "Tier", "tier", "CardTier", "cardTier", "Category", "category"
                    )

                    resultado = {
                        "scheme": validar_valor_bin(scheme_raw),
                        "issuer": validar_valor_bin(issuer_raw),
                        "type": validar_valor_bin(type_raw),
                        "tier": validar_valor_bin(tier_raw),
                    }
            else:
                logger.warning("Consulta BIN %s retornou HTTP %s", bin_code, resp.status_code)
    except Exception:
        logger.warning("Erro ao consultar BIN %s", bin_code, exc_info=True)

    if usar_cache:
        async with _BIN_CACHE_LOCK:
            _BIN_CACHE[bin_code] = dict(resultado)

    return dict(resultado)


def formatar_cartao_aprovado(
    numero: str, mes: str, ano: str, cvv: str, bin_info: dict[str, str]
) -> str:
    """Formata a linha final no padrão: numero|mes|ano|cvv|scheme|issuer|type|tier."""
    return (
        f"{numero}|{mes}|{ano}|{cvv}|"
        f"{bin_info['scheme']}|{bin_info['issuer']}|{bin_info['type']}|{bin_info['tier']}"
    )
