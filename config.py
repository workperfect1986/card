"""
config.py - Configurações avançadas do Unimar Card Tester
"""
import os
from pathlib import Path
from typing import Optional

class Settings:
    """Configurações centralizadas da aplicação."""
    
    # Autenticação
    EMAIL: str = os.getenv("UNIMAR_EMAIL", "")
    SENHA: str = os.getenv("UNIMAR_SENHA", "")
    NOME_FIXO: str = os.getenv("UNIMAR_NOME", "Bruna Mendes")
    
    # URLs
    URL_LOGIN: str = os.getenv("UNIMAR_URL_LOGIN", "https://digital.unimar.br/login")
    URL_CARTOES: str = os.getenv("UNIMAR_URL_CARTOES", "https://digital.unimar.br/areadoaluno/conta/meuscartoes")
    
    # Sessão
    SESSAO_PATH: Path = Path(os.getenv("UNIMAR_SESSAO_PATH", "sessao_unimar.json"))
    SESSAO_MAX_AGE: int = int(os.getenv("UNIMAR_SESSAO_MAX_AGE", "86400"))  # 24h
    
    # Workers
    MAX_TENTATIVAS: int = int(os.getenv("UNIMAR_MAX_TENTATIVAS", "3"))
    MAX_CANAIS: int = int(os.getenv("UNIMAR_MAX_CANAIS", "6"))
    
    # Timeouts (em ms)
    TIMEOUT_NAVEGACAO: int = int(os.getenv("UNIMAR_TIMEOUT_NAVEGACAO", "30000"))
    TIMEOUT_VALIDACAO: int = int(os.getenv("UNIMAR_TIMEOUT_VALIDACAO", "15000"))
    TIMEOUT_ELEMENTO: int = int(os.getenv("UNIMAR_TIMEOUT_ELEMENTO", "5000"))
    
    # Browser
    VIEWPORT_WIDTH: int = int(os.getenv("UNIMAR_VIEWPORT_WIDTH", "1280"))
    VIEWPORT_HEIGHT: int = int(os.getenv("UNIMAR_VIEWPORT_HEIGHT", "720"))
    HEADLESS: bool = os.getenv("UNIMAR_HEADLESS", "true").lower() == "true"
    
    USER_AGENT: str = os.getenv(
        "UNIMAR_USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )
    
    # Servidor
    HOST: str = os.getenv("UNIMAR_HOST", "0.0.0.0")
    PORT: int = int(os.getenv("UNIMAR_PORT", "5000"))
    
    # Rate Limiting
    RATE_LIMIT: str = os.getenv("UNIMAR_RATE_LIMIT", "10/minute")
    
    @property
    def browser_args(self) -> list:
        """Argumentos otimizados para o Chromium."""
        return [
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-web-security",
            "--disable-features=IsolateOrigins,site-per-process",
            "--disable-blink-features=AutomationControlled",
            "--disable-setuid-sandbox",
            "--no-first-run",
            "--no-zygote",
            "--single-process" if os.name != "nt" else "",  # Linux optimization
            "--disable-extensions",
            "--disable-background-networking",
            "--disable-sync",
            "--disable-translate",
            "--hide-scrollbars",
            "--metrics-recording-only",
            "--mute-audio",
            "--no-default-browser-check",
            "--disable-hang-monitor",
            "--disable-prompt-on-repost",
        ]

    @property
    def viewport(self) -> dict:
        return {"width": self.VIEWPORT_WIDTH, "height": self.VIEWPORT_HEIGHT}

# Singleton
settings = Settings()
