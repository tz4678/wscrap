# -*- coding: utf-8 -*-
"""Command line web scrapping tool"""
import argparse
import asyncio
import cgi
import dataclasses
import io
import json
import logging
import queue
import sys
from concurrent.futures import ProcessPoolExecutor
from typing import List, Optional, Set
from urllib.parse import urlsplit

import aiohttp

from .utils import Page, coro, normalize_url

__author__ = 'Sergey M <tz4678@gmail.com>'
__license__ = 'MIT'
__version__ = '0.1.0'

BANNER = r'''
██╗    ██╗███████╗ ██████╗██████╗  █████╗ ██████╗
██║    ██║██╔════╝██╔════╝██╔══██╗██╔══██╗██╔══██╗
██║ █╗ ██║███████╗██║     ██████╔╝███████║██████╔╝
██║███╗██║╚════██║██║     ██╔══██╗██╔══██║██╔═══╝
╚███╔███╔╝███████║╚██████╗██║  ██║██║  ██║██║
 ╚══╝╚══╝ ╚══════╝ ╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝
'''


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '-d', '--depth', default=3, help='crawl depth', type=int,
    )
    parser.add_argument(
        '-i',
        '--input',
        default=sys.stdin,
        help='input file',
        type=argparse.FileType('r'),
    )
    parser.add_argument(
        '-o',
        '--output',
        default=sys.stdout,
        help='output file',
        type=argparse.FileType('w'),
    )
    parser.add_argument(
        '-t', '--timeout', default=10.0, help='client timeout', type=float,
    )
    parser.add_argument(
        '-u',
        '--user-agent',
        default='Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0',
        help='client user agent',
    )
    parser.add_argument(
        '-v',
        '--verbosity',
        action='count',
        default=0,
        help='increase output verbosity: 0 - warning, 1 - info, 2 - debug',
    )
    parser.add_argument(
        '--version', action='version', version=f'v{__version__}'
    )
    parser.add_argument(
        '-w', '--workers', default=10, help='number of workers', type=int
    )
    return parser.parse_args(argv)


@coro
async def main(argv: Optional[List[str]] = None) -> Optional[int]:
    args = parse_args(argv)
    print(BANNER, file=sys.stderr, flush=True)
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    level = levels[min(args.verbosity, len(levels) - 1)]
    logging.basicConfig(level=level, stream=sys.stderr)
    urls = map(
        normalize_url, filter(None, map(str.strip, args.input.readlines())),
    )
    in_queue = asyncio.Queue()
    for url in urls:
        in_queue.put_nowait((url, args.depth))
    # используем ProcessPoolExecutor для парсинга html
    executor = ProcessPoolExecutor(args.workers)
    headers = {'User-Agent': args.user_agent}
    timeout = aiohttp.ClientTimeout(total=args.timeout)
    async with aiohttp.ClientSession(
        headers=headers, raise_for_status=True, timeout=timeout
    ) as session:
        visited = set()
        tasks = [
            # сразу запускает задание в фоновом режиме, которое можно остановить
            # вручную потом, вызвав cancel
            asyncio.create_task(
                worker(session, in_queue, visited, args.output, executor)
            )
            for _ in range(args.workers)
        ]
        await in_queue.join()
        logging.info('queue is empty')
        for task in tasks:
            task.cancel()
    logging.info('finished!')


async def worker(
    session: aiohttp.ClientSession,
    in_queue: asyncio.Queue,
    visited: Set[str],
    output: io.TextIOBase,
    executor: ProcessPoolExecutor,
) -> None:
    while True:
        # генерирует asyncio.CancelledError при отмене
        # если этот фрагмент разместить внутри try получим кучу ошибок:
        # ValueError: task_done() called too many times
        url, depth = await in_queue.get()
        try:
            if url in visited:
                logging.info('already visited: %s', url)
                continue
            logging.info('visit: %s', url)
            response = await session.get(url)
            cur_url = str(response.url)
            visited.add(url)
            visited.add(cur_url)
            ct, _ = cgi.parse_header(response.headers['content-type'])
            if ct != 'text/html':
                logging.warning('not html content: %s', cur_url)
                continue
            page = await Page.parse(response, executor)
            dumped = json.dumps(dataclasses.asdict(page), ensure_ascii=False)
            output.write(dumped)
            output.write('\n')
            output.flush()
            if depth > 0:
                domain = urlsplit(cur_url).netloc
                for link in page.links:
                    if domain == urlsplit(link['url']).netloc:
                        await in_queue.put((link['url'], depth - 1))
        except Exception as e:
            logging.warning(e)
        finally:
            in_queue.task_done()
