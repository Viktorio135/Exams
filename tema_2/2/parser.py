import os

import xlrd

from urllib.parse import urljoin
from urllib.request import urlopen, urlretrieve
from bs4 import BeautifulSoup
from datetime import datetime

from database.models import SpimexTradingResults
from database.database import Session, engine, Base


class ParserHTML:
    URL = 'https://spimex.com/markets/oil_products/trades/results'

    def __init__(self, download_dir='tema_2/2/files', count_pages=None):
        self.download_dir = download_dir
        self.count_pages = count_pages
        self._create_download_directory()

    def _create_download_directory(self):
        os.makedirs(self.download_dir, exist_ok=True)

    def _parse_page(self, url):
        try:
            response = urlopen(url)
            return BeautifulSoup(response.read(), "html.parser")
        except Exception as e:  # Укажите конкретные исключения (URLError, HTTPError)
            print(f"Ошибка загрузки страницы: {str(e)}")
            return

    def _download_file(self, url, filename):
        try:
            urlretrieve(url, filename)
        except Exception as e:
            print(f"Ошибка скачивания файла: {str(e)}")

    def _process_block(self, block):
        date_element = block.find('span')

        date_str = date_element.text.strip()
        date_obj = datetime.strptime(date_str, "%d.%m.%Y")
                    
        # Проверяем год
        if date_obj.year < 2023:
            return
                
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
        self._download_file(file_url, filename)
        
    def parse(self):
        page = 1
        while True:
            page_url = self.URL + f'/?page=page-{page}'
            soup = self._parse_page(page_url)
            target_links = soup.find_all(
                'a',
                class_='accordeon-inner__item-title',
                string='Бюллетень по итогам торгов в Секции «Нефтепродукты»'
            )

            if len(target_links) == 0:
                break
            
            blocks = [link.find_parent('div', class_='accordeon-inner__item') for link in target_links]
            for block in blocks:
                self._process_block(block)

            if self.count_pages:
                if page == self.count_pages:
                    break
            page += 1
        
class ParserExcel:
    def __init__(self, input_dir):
        self.input_dir = input_dir

    def parse_files(self):
        for file in os.listdir(self.input_dir):
            if file.endswith(".xls"):
                self.parse_file(os.path.join(self.input_dir, file))


    def parse_file(self, path):
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
                    trade_date = datetime.strptime(date_str, "%d.%m.%Y").strftime("%Y-%m-%d")
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

            except (IndexError, ValueError, TypeError) as e:
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

        self._save_date_to_db(data)


    def _convert(self, value, target_type):
        if value in ('', '-', None):
            return 0
        
        try:
            if target_type == int and isinstance(value, float):
                if not value.is_integer():
                    raise ValueError("Дробное значение для целого поля")
                return int(value)
                
            return target_type(value)
        except (ValueError, TypeError) as e:
            pass
        
    
    def _save_date_to_db(self, data):
        with Session(autoflush=False, bind=engine) as db:
            try:
                db.bulk_insert_mappings(SpimexTradingResults, data)
                db.commit()
            except Exception as e:
                print(f"Ошибка записи в БД: {e}")
                db.rollback()


if __name__=='__main__':
    Base.metadata.create_all(bind=engine)
    parser_html = ParserHTML(count_pages=1) #указывается количестов страниц (если не указать, то будут читаться все)
    parser_html.parse()
    parser_excel = ParserExcel(parser_html.download_dir)
    parser_excel.parse_files()
