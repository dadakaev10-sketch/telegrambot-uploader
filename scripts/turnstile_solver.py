#!/usr/bin/env python3
"""Cloudflare Turnstile token helper for SerienStream player gates.

Runs a real Chromium via nodriver on Xvfb and prints only the token to stdout.
"""
from __future__ import annotations

import asyncio
import json
import os
import platform
import random
import subprocess
import sys
import time
from typing import Optional

import nodriver as uc


def _find_chrome() -> str:
    if os.environ.get("CHROME_PATH"):
        return os.environ["CHROME_PATH"]
    for path in (
        "/usr/bin/google-chrome-stable",
        "/usr/bin/google-chrome",
        "/usr/bin/chromium-browser",
        "/usr/bin/chromium",
        "/snap/bin/chromium",
    ):
        if os.path.exists(path):
            return path
    raise FileNotFoundError("Chrome/Chromium not found; set CHROME_PATH")


def _profile_dir() -> str:
    return os.environ.get("TS_PROFILE_DIR", "/opt/workspaces/telegrambot-uploader/bot/ts_profile")


def _start_xvfb_if_needed() -> Optional[subprocess.Popen]:
    if platform.system() != "Linux" or os.environ.get("DISPLAY"):
        return None
    proc = subprocess.Popen(
        ["Xvfb", ":99", "-screen", "0", "1280x900x24"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    os.environ["DISPLAY"] = ":99"
    time.sleep(0.5)
    return proc


async def _solve(sitekey: str, siteurl: str, timeout: int) -> str:
    browser = await uc.start(
        browser_executable_path=_find_chrome(),
        headless=False,
        user_data_dir=_profile_dir(),
    )
    try:
        page = await browser.get(siteurl)
        await asyncio.sleep(random.uniform(2.0, 3.0))
        await page.evaluate(f"""
            (() => {{
                if (document.getElementById('_ts_box')) return;
                window._tsToken = null;
                const wrap = document.createElement('div');
                wrap.id = '_ts_box';
                wrap.style = 'position:fixed;top:20px;left:20px;z-index:2147483647;';
                document.body.appendChild(wrap);
                window._tsLoad = function () {{
                    turnstile.render('#_ts_box', {{
                        sitekey: '{sitekey}',
                        callback: function(token) {{ window._tsToken = token; }}
                    }});
                }};
                const s = document.createElement('script');
                s.src = 'https://challenges.cloudflare.com/turnstile/v0/api.js?onload=_tsLoad&render=explicit';
                s.async = true;
                document.head.appendChild(s);
            }})();
        """)
        await asyncio.sleep(5.0)

        async def get_token() -> Optional[str]:
            return await page.evaluate("""
                (() => {
                    if (window._tsToken) return window._tsToken;
                    const inp = document.querySelector('#_ts_box [name="cf-turnstile-response"]');
                    return (inp && inp.value) ? inp.value : null;
                })()
            """)

        async def get_iframe_rect() -> Optional[dict]:
            raw = await page.evaluate("""
                JSON.stringify((() => {
                    for (const f of document.querySelectorAll('iframe')) {
                        const src = f.src || f.getAttribute('src') || '';
                        if (!src.includes('challenges.cloudflare.com')) continue;
                        const r = f.getBoundingClientRect();
                        if (r.width > 50 && r.height > 20) return {x:r.x, y:r.y, w:r.width, h:r.height};
                    }
                    return null;
                })())
            """)
            return json.loads(raw) if raw and raw != 'null' else None

        async def do_click(rect: Optional[dict]) -> None:
            if rect:
                cx = rect["x"] + 28 + random.uniform(-3, 3)
                cy = rect["y"] + rect["h"] / 2 + random.uniform(-3, 3)
            else:
                cx = 20 + 28 + random.uniform(-3, 3)
                cy = 20 + 32 + random.uniform(-3, 3)
            await page.mouse_move(cx - 80, cy - 20)
            await asyncio.sleep(random.uniform(0.15, 0.25))
            await page.mouse_move(cx, cy)
            await asyncio.sleep(random.uniform(0.08, 0.15))
            await page.mouse_click(cx, cy)

        token = await get_token()
        if token:
            return token
        rect = None
        for _ in range(20):
            rect = await get_iframe_rect()
            if rect:
                break
            await asyncio.sleep(0.5)
        deadline = asyncio.get_event_loop().time() + timeout
        click_count = 0
        last_click = 0.0
        while asyncio.get_event_loop().time() < deadline:
            token = await get_token()
            if token:
                return token
            now = asyncio.get_event_loop().time()
            if click_count == 0 or now - last_click > 8:
                if click_count < 3:
                    await do_click(rect)
                    last_click = asyncio.get_event_loop().time()
                    click_count += 1
                    await asyncio.sleep(1.0)
                    rect = await get_iframe_rect() or rect
                    continue
            await asyncio.sleep(0.3)
    finally:
        try:
            browser.stop()
        except Exception:
            pass
    raise TimeoutError(f"Turnstile token not obtained within {timeout}s")


def main() -> int:
    if len(sys.argv) < 3:
        print("Usage: turnstile_solver.py <sitekey> <siteurl> [timeout]", file=sys.stderr)
        return 2
    xvfb = _start_xvfb_if_needed()
    try:
        token = asyncio.run(_solve(sys.argv[1], sys.argv[2], int(sys.argv[3]) if len(sys.argv) > 3 else 60))
        print(token)
        return 0
    finally:
        if xvfb:
            xvfb.terminate()


if __name__ == "__main__":
    raise SystemExit(main())
