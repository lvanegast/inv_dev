"""Nodo de automatización de navegador Web (RPA) con Playwright."""

import urllib.parse
from pathlib import Path
from typing import Any

from flowengine.nodes.base import BaseNode


class BrowserAction(BaseNode):
    """Ejecuta flujos de automatización robótica (RPA) en navegadores web (Chromium/Playwright)."""

    async def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        cfg = self.resolve_config(context)

        # Si el nodo está marcado con mock (testing / CI sin display)
        if cfg.get("mock", False):
            return {
                "status": "ok",
                "mocked": True,
                "url": cfg.get("url", ""),
                "title": "Mock Browser Page",
                "extracted": cfg.get("mock_extracted", {}),
                "actions_executed": len(cfg.get("actions", [])),
            }

        url = cfg.get("url")
        html_content = cfg.get("html")
        if not url and not html_content:
            raise ValueError(f"Nodo '{self.node_id}' requiere el parámetro 'url' o 'html'.")

        # Codificar de forma segura data URLs con HTML para evitar truncamiento por caracteres '#'
        if url and url.startswith("data:text/html") and "," in url:
            header, raw_content = url.split(",", 1)
            if "<" in raw_content or "#" in raw_content:
                url = f"{header},{urllib.parse.quote(raw_content)}"

        actions = cfg.get("actions", [])
        headless = bool(cfg.get("headless", True))
        timeout_ms = int(cfg.get("timeout_ms", 10000))
        viewport = cfg.get("viewport", {"width": 1280, "height": 800})

        try:
            from playwright.async_api import async_playwright
        except ImportError as e:
            raise RuntimeError(
                f"Playwright no está disponible en este entorno: {e}. "
                "Ejecuta 'uv add playwright && uv run playwright install chromium'."
            ) from e

        extracted: dict[str, Any] = {}
        screenshot_path: str | None = None
        screenshot_url: str | None = None
        executed_steps: list[str] = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=headless,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
            )
            page = await browser.new_page(
                viewport=viewport,
            )
            page.set_default_timeout(timeout_ms)

            # 1. Cargar el documento o navegar a la URL principal
            if html_content:
                await page.set_content(html_content, wait_until="domcontentloaded")
                executed_steps.append("set_content: inline html")
            elif url:
                await page.goto(url, wait_until=cfg.get("wait_until", "domcontentloaded"))
                executed_steps.append(f"goto: {url[:100]}...")

            # 2. Ejecutar la secuencia de acciones RPA
            for step in actions:
                action_type = step.get("action", "").lower().strip()

                if action_type in ("fill", "type"):
                    sel = step.get("selector")
                    val = str(step.get("value", ""))
                    await page.fill(sel, val)
                    executed_steps.append(f"fill: {sel}")

                elif action_type == "click":
                    sel = step.get("selector")
                    await page.click(sel)
                    executed_steps.append(f"click: {sel}")

                elif action_type == "press":
                    sel = step.get("selector")
                    key = step.get("key", "Enter")
                    if sel:
                        await page.press(sel, key)
                    else:
                        await page.keyboard.press(key)
                    executed_steps.append(f"press: {key}")

                elif action_type == "select":
                    sel = step.get("selector")
                    val = step.get("value")
                    await page.select_option(sel, val)
                    executed_steps.append(f"select: {sel} -> {val}")

                elif action_type == "wait_for_selector":
                    sel = step.get("selector")
                    state = step.get("state", "visible")
                    await page.wait_for_selector(sel, state=state)
                    executed_steps.append(f"wait_for: {sel}")

                elif action_type == "wait_timeout":
                    ms = int(step.get("ms", 1000))
                    await page.wait_for_timeout(ms)
                    executed_steps.append(f"wait_timeout: {ms}ms")

                elif action_type in ("extract_text", "get_text"):
                    sel = step.get("selector")
                    key = step.get("key", sel)
                    el = await page.wait_for_selector(sel)
                    text = (await el.inner_text()) if el else ""
                    extracted[key] = text.strip()
                    executed_steps.append(f"extract_text: {sel} -> {key}")

                elif action_type in ("extract_attribute", "get_attribute"):
                    sel = step.get("selector")
                    attr = step.get("attribute", "href")
                    key = step.get("key", f"{sel}_{attr}")
                    el = await page.wait_for_selector(sel)
                    val = (await el.get_attribute(attr)) if el else None
                    extracted[key] = val
                    executed_steps.append(f"extract_attribute: {sel}[{attr}]")

                elif action_type == "screenshot":
                    path = step.get("path", f"screenshots/rpa_{self.node_id}.png")
                    Path(path).parent.mkdir(parents=True, exist_ok=True)
                    full_page = bool(step.get("full_page", False))
                    await page.screenshot(path=path, full_page=full_page)
                    screenshot_path = str(Path(path).resolve())
                    screenshot_url = f"/screenshots/{Path(path).name}"
                    executed_steps.append(f"screenshot: {path}")

                elif action_type == "evaluate":
                    script = step.get("script", "")
                    res = await page.evaluate(script)
                    key = step.get("key", "eval_result")
                    extracted[key] = res
                    executed_steps.append(f"evaluate -> {key}")

                else:
                    raise ValueError(f"Acción RPA desconocida: '{action_type}'")

            final_title = await page.title()
            final_url = page.url
            await browser.close()

        return {
            "status": "ok",
            "url": final_url,
            "title": final_title,
            "extracted": extracted,
            "screenshot_path": screenshot_path,
            "screenshot_url": screenshot_url,
            "actions_executed": len(executed_steps),
            "steps": executed_steps,
        }
