import os
from aiohttp import web

_runner = None

async def start_health_server():
    global _runner

    async def health(request):
        return web.Response(text="OK")

    app = web.Application()
    app.router.add_get("/", health)

    _runner = web.AppRunner(app)
    await _runner.setup()

    port = int(os.getenv("PORT", "8000"))

    site = web.TCPSite(
        _runner,
        "0.0.0.0",
        port
    )

    await site.start()

    print(f"Health server started on port {port}")
