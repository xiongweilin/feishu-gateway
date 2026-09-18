from __future__ import annotations

import socket

import uvicorn

from .app import create_app
from .config import Settings
from .json_logging import configure_logging
from .telemetry import configure_telemetry


def bind_socket(host: str, port: int) -> socket.socket:
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    listener = socket.socket(family, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((host, port))
    listener.listen(2_048)
    listener.setblocking(False)
    return listener


def main() -> None:
    configure_logging()
    settings = Settings.from_environment()
    if settings.port == settings.public_port:
        raise RuntimeError("Internal and public gateway ports must be different")
    app = create_app(settings)
    configure_telemetry(app)
    listeners = [
        bind_socket(settings.host, settings.port),
        bind_socket(settings.host, settings.public_port),
    ]
    try:
        config = uvicorn.Config(app, host=settings.host, port=settings.port, log_config=None)
        uvicorn.Server(config).run(sockets=listeners)
    finally:
        for listener in listeners:
            listener.close()


if __name__ == "__main__":
    main()
