#!/usr/bin/env python3
"""Application entry point.

The legacy bot module remains the compatibility composition root while services
are moved incrementally into app/.
"""
import asyncio

from bot import main


if __name__ == '__main__':
    asyncio.run(main())
