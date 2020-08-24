# -*- coding: utf-8 -*-
import argparse
import asyncio
from concurrent.futures._base import Executor
from dataclasses import dataclass
from functools import partial, wraps
from html import unescape
from typing import Any, Callable, Dict, List, Optional, Tuple, Type
from urllib.parse import urldefrag, urljoin, urlsplit

import aiohttp
from bs4 import BeautifulSoup


def coro(f: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(f)
    def wrapper(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


def normalize_url(url: str) -> str:
    return url if '://' in url else 'http://' + url


@dataclass
class Page:
    url: str
    title: str
    links: List[Dict[str, str]]

    @classmethod
    async def parse(
        cls,
        response: aiohttp.ClientResponse,
        executor: Optional[Executor] = None,
    ) -> 'Page':
        url = str(response.url)
        html = await response.text()
        title, links = await call_in_executor(
            parse_page, url, html, executor=executor
        )
        return cls(url, title, links)


def parse_page(url: str, html: str) -> Tuple[str, List[Dict[str, str]]]:
    soup = BeautifulSoup(html, 'lxml')
    title = unescape(soup.title.text.strip())
    links = parse_links(url, soup)
    return title, links


def parse_links(url: str, soup: BeautifulSoup) -> List[Dict[str, str]]:
    rv = []
    for link in soup.find_all('a', download=False, href=True):
        href = urljoin(url, link['href'])
        if is_resource(href):
            continue
        rv.append(
            {'url': urldefrag(href)[0], 'text': unescape(link.text.strip()),}
        )
    return rv


RESOURCE_EXTENSIONS = (
    '.ai',
    '.avi',
    '.bin',
    '.bmp',
    '.deb',
    '.doc',
    '.docx',
    '.exe',
    '.flv',
    '.gif',
    '.gz',
    '.jpeg',
    '.jpg',
    '.mkv',
    '.mov',
    '.mp3',
    '.mp4',
    '.odt',
    '.ogg',
    '.pdf',
    '.png',
    '.psd',
    '.rar',
    '.rpm'
    # '.tar.gz',
    '.ts',
    '.txt',
    '.webm',
    '.xml',
    '.xslx',
    '.xz',
    '.zip',
)


def is_resource(url: str) -> bool:
    return urlsplit(url).path.endswith(RESOURCE_EXTENSIONS)


def call_in_executor(
    func: Callable,
    *args: Tuple[Any, ...],
    executor: Optional[Executor] = None,
    **kwargs: Dict[str, Any]
) -> asyncio.Future:
    callback = partial(func, *args, **kwargs)
    return asyncio.get_event_loop().run_in_executor(executor, callback)
