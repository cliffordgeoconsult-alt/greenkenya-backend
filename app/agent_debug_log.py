import json
import time
from pathlib import Path

_LOG_PATH = Path(__file__).resolve().parent.parent / "debug-2b0fd4.log"
_SESSION = "2b0fd4"


def agent_debug_log(hypothesis_id: str, location: str, message: str, data=None):
    # #region agent log
    try:
        payload = {
            "sessionId": _SESSION,
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data or {},
            "timestamp": int(time.time() * 1000),
        }
        with _LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, default=str) + "\n")
    except Exception:
        pass
    # #endregion
