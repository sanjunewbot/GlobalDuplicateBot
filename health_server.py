from aiohttp import web
import os

async def health(request):
    return web.Response(text="OK")

async def start_health_server():
    app = web.Application()
    app.router.add_get("/", health)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.getenv("PORT", "8000"))

    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    print(f"Health server running on port {port}")
