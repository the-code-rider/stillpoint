import os
import uvicorn

def main():
    host = os.getenv("OBS_HOST", "127.0.0.1")
    port = int(os.getenv("OBS_PORT", "7000"))
    reload_ = os.getenv("OBS_RELOAD", "0") == "1"

    uvicorn.run(
        "stillpoint.collector_app:app",
        host=host,
        port=port,
        reload=reload_,
        log_level=os.getenv("OBS_LOG_LEVEL", "info"),
    )
