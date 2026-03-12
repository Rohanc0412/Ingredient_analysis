import asyncio

from .downloader import main


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
