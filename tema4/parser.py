from concurrent.futures import ThreadPoolExecutor
import os
import aiohttp
import asyncio
import aiofiles

from sqlalchemy import insert
import xlrd

from urllib.parse import urljoin
from bs4 import BeautifulSoup
from datetime import datetime

from database.database import engine, Base, AsyncSessionLocal
from database.models import SpimexTradingResults


class ParserHTML:
    URL = 'https://spimex.com/markets/oil_products/trades/results'

    def __init__(self, download_dir='/Users/viktorshpakovskij/работа/tema_2/tema4/files', count_pages=None):
        self.download_dir = download_dir
        self.count_pages = count_pages
        self._create_download_directory()
        self.semaphore = asyncio.Semaphore(10)

    def _create_download_directory(self):
        os.makedirs(self.download_dir, exist_ok=True)

    async def _parse_page(self, url, session):
        try:
            async with session.get(url) as response:
                html = await response.read()
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, BeautifulSoup, html, "html.parser")
        except Exception as e:
            print(f"Ошибка загрузки страницы: {str(e)}")
            return

    async def _download_file(self, url, filename, session):
        try:
            async with self.semaphore:
                async with session.get(url) as response:
                    if response.status == 200:
                        async with aiofiles.open(filename, 'wb') as f:
                            async for chunk in response.content.iter_chunked(1024 * 16):
                                await f.write(chunk)
        except Exception as e:
            print(f"Ошибка скачивания файла: {str(e)}")

    async def _process_block(self, block, session):
        date_element = block.find('span')

        date_str = date_element.text.strip()
        date_obj = datetime.strptime(date_str, "%d.%m.%Y")
                    
        if date_obj.year < 2023:
            return 'stop'
                
        link = block.find('a', class_='accordeon-inner__item-title')
        if not link:
            return

        href = link.get('href')
        if not href:
            return

        file_url = urljoin(self.URL, href)

        filename = os.path.join(
            self.download_dir,
            f"oil_report_{date_obj.strftime('%Y-%m-%d')}.xls"
        )
        await self._download_file(file_url, filename, session)
        
    async def parse(self, page, session):
        page_url = self.URL + f'/?page=page-{page}'
        soup = await self._parse_page(page_url, session)
        if not soup:
            return
        target_links = soup.find_all(
            'a',
            class_='accordeon-inner__item-title',
            string='Бюллетень по итогам торгов в Секции «Нефтепродукты»'
        )

        if len(target_links) == 0:
            return
        blocks = [link.find_parent('div', class_='accordeon-inner__item') for link in target_links]
        await asyncio.gather(*[self._process_block(block, session) for block in blocks])

    async def start_aio_parse(self):
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*[self.parse(page, session) for page in range(1, self.count_pages)])

    async def start(self):
        await self.start_aio_parse()


class ParserExcel:
    def __init__(self, input_dir: str):
        self.input_dir = input_dir
        self.db_semaphore = asyncio.Semaphore(10)
        self.executor = ThreadPoolExecutor(max_workers=4)

    def _parse_excel(self, path: str) -> list:
        try:
            workbook = xlrd.open_workbook(path)
            sheet = workbook.sheet_by_index(0)

            trade_date = None
            data = []
            start_data_row = 0

            for row_idx in range(sheet.nrows):
                row = sheet.row_values(row_idx)
                if not trade_date and "Дата торгов:" in row[1]:
                    date_str = row[1].split(":")[1].strip()
                    try:
                        date_obj = datetime.strptime(date_str, "%d.%m.%Y")
                        trade_date = date_obj.date()
                    except ValueError:
                        trade_date = "1970-01-01"
                        print(f"Неверный формат даты в файле {path}")

                if "Единица измерения: Метрическая тонна" in row[1]:
                    start_data_row = row_idx + 3
                    break

            for row_idx in range(start_data_row, sheet.nrows):
                row = sheet.row_values(row_idx)

                try:
                    code = str(row[1]).strip() if row[1] else None
                    name = str(row[2]).strip() if row[2] else None
                    basis = str(row[3]).strip() if row[3] else None

                    if not all([code, name, basis]):
                        continue
                except IndexError:
                    continue

                try:
                    volume = self._convert(row[4], int)
                    if volume < 0:
                        continue

                    total = self._convert(row[5], int)
                    if total < 0:
                        continue

                    count = self._convert(row[14], int)
                    if count <= 0:
                        continue

                    data.append({
                        "exchange_product_id": code,
                        "exchange_product_name": name,
                        "delivery_basis_name": basis,
                        "volume": volume,
                        "total": total,
                        "count": count,
                        "date": trade_date
                    })

                except Exception as e:
                    print(f"Ошибка обработки строки {row_idx} в файле {path}: {str(e)}")
                    continue

            return data

        except Exception as e:
            print(f"Критическая ошибка в файле {path}: {str(e)}")
            return []

    def _convert(self, value, target_type):
        if value in ('', '-', None):
            return 0
        try:
            if target_type == int and isinstance(value, float):
                return int(value) if value.is_integer() else 0
            return target_type(value)
        except (ValueError, TypeError):
            return 0

    async def _save_to_db(self, data: list):
        if not data:
            return

        async with self.db_semaphore:
            try:
                async with AsyncSessionLocal() as session:
                    stmt = insert(SpimexTradingResults).values(data)
                    await session.execute(stmt)
                    await session.commit()
                    print(f"Успешно сохранено {len(data)} записей")
            except Exception as e:
                print(f"Ошибка БД: {str(e)}")
                await session.rollback()
                raise

    async def process_file(self, file_path: str):
        try:
            data = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                self._parse_excel,
                os.path.join(self.input_dir, file_path)
            )

            if data:
                await self._save_to_db(data)

        except Exception as e:
            print(f"Ошибка обработки файла {file_path}: {str(e)}")

    async def start(self):
        files = [
            os.path.join(self.input_dir, f)
            for f in os.listdir(self.input_dir)
            if f.endswith('.xls')
        ]

        await asyncio.gather(*[self.process_file(file) for file in files])


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    html_parser = ParserHTML(
        count_pages=57
    )
    await html_parser.start()

    await asyncio.sleep(2)

    excel_parser = ParserExcel(
        input_dir=html_parser.download_dir,
    )
    await excel_parser.start()


if __name__ == '__main__':
    asyncio.run(main())
