from __future__ import annotations

import asyncio
import logging


async def _handle_connection(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    """
    Handle one HTTP connection with the smallest possible valid
    response. Reads (and discards) whatever request comes in, then
    always replies 200 OK — the pinger only cares that *something*
    answered, not what path or method it used.
    """
    try:
        try:
            await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5)
        except (asyncio.IncompleteReadError, asyncio.TimeoutError, ConnectionError):
            pass  # malformed/partial request is fine; still respond 200 below

        body = b"OK"
        response = (
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/plain\r\n"
            b"Content-Length: " + str(len(body)).encode("ascii") + b"\r\n"
            b"Connection: close\r\n"
            b"\r\n" + body
        )
        writer.write(response)
        await writer.drain()
    except (ConnectionError, OSError):
        pass
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except (ConnectionError, OSError):
            pass


async def run_health_server(port: int, logger: logging.Logger, host: str = "0.0.0.0") -> None:
    """
    Serve a trivial 200-OK HTTP response on `port` forever. Intended to
    be run as a background asyncio task (e.g. via `asyncio.create_task`)
    alongside the bot's other background tasks; cancel the task to stop
    it during shutdown.

    Raises whatever `asyncio.start_server` raises (e.g. OSError if the
    port is already in use) so the caller's task-level error handling
    surfaces the problem rather than silently swallowing it.
    """
    server = await asyncio.start_server(_handle_connection, host=host, port=port)
    logger.info(
        "Keep-alive HTTP endpoint listening on %s:%s (for uptime pingers / platform health checks).",
        host, port,
    )
    async with server:
        await server.serve_forever()
        
