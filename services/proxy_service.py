import asyncio
import logging
import aiohttp
from aiohttp_socks import ProxyConnector
from os import path, remove
from glob import glob
from config.config import Config
from repositories.proxy_repository import ProxyRepository

class ProxyService:

    @staticmethod
    async def check_proxy(proxy_url: str) -> bool:
        """Универсально проверяет доступность Telegram API через HTTP или SOCKS5."""
        try:
            timeout = aiohttp.ClientTimeout(total=Config.TIME_OF_CHECKING_PROXY_SECONDS)

            # Если это SOCKS5, используем коннектор ProxyConnector
            if proxy_url.startswith("socks5://"):
                connector = ProxyConnector.from_url(proxy_url)
                async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                    async with session.get("https://telegram.org", allow_redirects=False) as resp:
                        return resp.status in [200, 404]

            # If это обычный HTTP/HTTPS прокси, передаем его напрямую в аргумент proxy
            else:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get("https://telegram.org", proxy=proxy_url, allow_redirects=False) as resp:
                        return resp.status in [200, 404]
        except Exception:
            return False

    @classmethod
    async def find_working_proxy_local_scrape(cls) -> list[str]:
        """Главная точка входа: координирует процесс парсинга папки."""
        search_pattern = path.join(Config.FOLDER_PATH, "*.txt")
        file_paths = glob(search_pattern)

        if not file_paths:
            logging.info(f"Папка '{Config.FOLDER_PATH}' пуста. Нет файлов для парсинга.")
            return []

        logging.info(f"Обнаружено файлов для обработки: {len(file_paths)}")

        # Создаем общую очередь для связи воркеров и потребителя базы данных
        queue = asyncio.Queue()
        working_proxies = []

        # 1. Запускаем фонового потребителя для записи батчей в БД
        saver_task = asyncio.create_task(cls._db_saver_consumer(queue, working_proxies))

        # 2. Обрабатываем каждый файл
        for file_path in file_paths:
            await cls._process_single_file(file_path, queue)

        # 3. Завершаем работу: посылаем сигнал стопа и ждем окончания записи
        await queue.put(None)
        await saver_task

        return working_proxies

    @classmethod
    async def _process_single_file(cls, file_path: str, queue: asyncio.Queue) -> None:
        """Отвечает только за чтение одного файла, фильтрацию и запуск параллельной проверки."""
        logging.info(f"Начинаю обработку файла: {file_path}")
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                lines = file.readlines()

            logging.info(f"Успешно прочитано {len(lines)} строк из {path.basename(file_path)}")

            # Фильтруем строки и убираем дубликаты
            raw_urls = {
                url for line in lines
                if
                (url := line.strip()) and not url.startswith("#") and "://" in url and not url.startswith("socks4://")}

            # Запускаем параллельную проверку пулом в 50 одновременных запросов
            semaphore = asyncio.Semaphore(50)
            tasks = [cls._worker(url, semaphore, queue) for url in raw_urls]
            await asyncio.gather(*tasks)

            remove(file_path)
            logging.info(f"Файл {file_path} успешно обработан параллельно и удален.")

        except Exception as e:
            logging.error(f"Ошибка при обработке файла {file_path}: {e}")

    @classmethod
    async def _worker(cls, url: str, semaphore: asyncio.Semaphore, queue: asyncio.Queue) -> None:
        """Отвечает за проверку одного конкретного прокси и отправку его в очередь."""
        async with semaphore:
            if await cls.check_proxy(url):
                logging.info(f"Рабочий прокси найден: {url}")
                queue.put_nowait(url)

    @classmethod
    async def _db_saver_consumer(cls, queue: asyncio.Queue, working_proxies: list[str]) -> None:
        """Фоновый метод: слушает очередь и сохраняет прокси батчами по 10 штук в БД."""
        batch = []
        while True:
            try:
                proxy_url = await queue.get()
                # Сигнал завершения (poison pill)
                if proxy_url is None:
                    if batch:
                        await ProxyRepository.save_working_proxies(batch)
                    queue.task_done()
                    break

                batch.append(proxy_url)
                working_proxies.append(proxy_url)
                # Запись батча при накоплении 10 штук
                if len(batch) >= 10:
                    logging.info(f"Накоплен батч из {len(batch)} прокси. Сохраняю в БД...")
                    await ProxyRepository.save_working_proxies(batch)
                    batch.clear()

                queue.task_done()
            except Exception as e:
                logging.error(f"Ошибка в фоновом сохранителе БД: {e}")














