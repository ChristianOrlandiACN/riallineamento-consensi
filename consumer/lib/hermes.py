import os
import base64
import logging
import urllib.request
import urllib.error
from .secrets import get_secret

logger = logging.getLogger()

HERMES_BASE_URL = os.environ["HERMES_BASE_URL"].rstrip("/")
HERMES_SECRET_NAME = os.environ.get("HERMES_SECRET_NAME", "")
HERMES_TIMEOUT_S: float = int(os.environ.get("HERMES_TIMEOUT_MS", "10000")) / 1000

# ─── Basic Auth ───────────────────────────────────────────────────────────────
# TODO: attivare quando il team MDW conferma l'autenticazione Basic Auth.
# Impostare env var HERMES_SECRET_NAME con il nome del secret Secrets Manager
# (es. prod/hermes/call-me-now) che contiene i campi "username" e "password".
# ─────────────────────────────────────────────────────────────────────────────
_auth_header: str | None = None


def _get_auth_header() -> str | None:
    global _auth_header
    if _auth_header is not None:
        return _auth_header
    if not HERMES_SECRET_NAME:
        return None
    creds = get_secret(HERMES_SECRET_NAME)
    token = base64.b64encode(
        f"{creds['username']}:{creds['password']}".encode()
    ).decode()
    _auth_header = f"Basic {token}"
    return _auth_header


class HermesError(Exception):
    def __init__(self, message: str, status_code: int | None, is_recoverable: bool):
        super().__init__(message)
        self.status_code = status_code
        self.is_recoverable = is_recoverable


def update_consent(record: dict) -> None:
    """
    PUT /devices/{decoder_id}/opt-out  se hasoptedoutrecommendation__c == 'true'
    PUT /devices/{decoder_id}/opt-in   se hasoptedoutrecommendation__c == 'false'

    Errori 5xx / network timeout → HermesError(is_recoverable=True)  → retry SQS → DLQ
    Errori 4xx               → HermesError(is_recoverable=False) → scarto
    """
    decoder_id = record["decoder_id_original"]
    opted_out = str(record.get("hasoptedoutrecommendation__c", "")).lower() == "true"
    action = "opt-out" if opted_out else "opt-in"
    url = f"{HERMES_BASE_URL}/devices/{decoder_id}/{action}"

    req = urllib.request.Request(url, data=b"", method="PUT")
    req.add_header("Content-Type", "application/json")

    # TODO: decommentare il blocco seguente una volta confermata la Basic Auth
    # auth = _get_auth_header()
    # if auth:
    #     req.add_header("Authorization", auth)

    logger.info("PUT %s", url)
    try:
        with urllib.request.urlopen(req, timeout=HERMES_TIMEOUT_S) as resp:
            logger.info("Hermes %d for decoder %s", resp.status, decoder_id)
            return resp.status
    except urllib.error.HTTPError as e:
        is_recoverable = e.code >= 500
        raise HermesError(
            f"Hermes {action} HTTP {e.code} for decoder {decoder_id}",
            status_code=e.code,
            is_recoverable=is_recoverable,
        )
    except (urllib.error.URLError, TimeoutError) as e:
        raise HermesError(
            f"Hermes {action} network error for decoder {decoder_id}: {e}",
            status_code=None,
            is_recoverable=True,
        )
