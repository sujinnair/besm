from __future__ import annotations

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright


class ParsedHTML:
    __slots__ = ("text", "title", "url")

    def __init__(self, text: str, title: str, url: str) -> None:
        self.text = text
        self.title = title
        self.url = url


async def fetch_and_parse(url: str, timeout_ms: int = 30_000) -> ParsedHTML:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, timeout=timeout_ms, wait_until="domcontentloaded")
        html = await page.content()
        await browser.close()

    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "noscript", "header", "footer", "nav"]):
        tag.decompose()

    title = soup.title.string.strip() if soup.title and soup.title.string else ""
    text = soup.get_text(separator="\n", strip=True)

    return ParsedHTML(text=text, title=title, url=url)
