import asyncio
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from config.config import Config
from config.database import engine, Base
from services.proxy_service import ProxyService
from repositories.proxy_repository import ProxyRepository
from services.email_service import EmailService

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

async def init_db_tables():
    """Аналог ddl-auto: update. Проверяет наличие таблиц в схеме и создает их, если их нет."""
    logging.info("Проверка и инициализация таблиц в базе данных...")
    try:
        async with engine.begin() as conn:
            # Выполняем синхронную функцию создания таблиц в асинхронном контексте движка
            await conn.run_sync(Base.metadata.create_all)
        logging.info("Инициализация таблиц успешно завершена.")
    except Exception as e:
        logging.error(f"Критическая ошибка при создании таблиц: {e}")
        raise e


class ProxySchedulerTasks:

    @staticmethod
    async def check_folder_job():
        """Задача 1: Поиск новых файлов в папке."""
        # Вызываем метод, который мы переписывали ранее
        working_proxies = await ProxyService.find_working_proxy_local_scrape()
        if working_proxies:
            await ProxyRepository.save_working_proxies(working_proxies)

    @staticmethod
    async def check_database_proxies_job():
        """Задача 2: Регулярная проверка прокси в БД по интервалу."""
        logging.info("Начало плановой проверки прокси в базе данных...")

        proxies = await ProxyRepository.get_all_proxies()
        if not proxies:
            logging.info("База данных прокси пуста. Проверка не требуется.")
            await EmailService.send_alarm_email(0)
            return

        # 2. Создаем очередь в памяти и список для накопления результатов
        queue = asyncio.Queue()

        # 3. Пишем фонового потребителя, который будет собирать результаты и писать в БД пачками по 10 штук
        async def db_saver_consumer():
            batch = []
            while True:
                try:
                    # Ждем данные из воркеров
                    item = await queue.get()

                    # Сигнал завершения (poison pill)
                    if item is None:
                        if batch:
                            await ProxyRepository.update_proxies_batch(batch)
                        queue.task_done()
                        break

                    batch.append(item)

                    # Как только накопили 10 штук — пушим батч в базу
                    if len(batch) >= 10:
                        await ProxyRepository.update_proxies_batch(batch)
                        batch.clear()

                    queue.task_done()
                except Exception as e:
                    logging.error(f"Ошибка в фоновом сохранителе проверок БД: {e}")

        # Запускаем потребителя базы данных в фоне
        saver_task = asyncio.create_task(db_saver_consumer())

        # 4. Пишем воркер для проверки (он не трогает БД, а только пушит данные в очередь)
        semaphore = asyncio.Semaphore(20)

        async def db_proxy_worker(proxy):
            async with semaphore:
                is_working = await ProxyService.check_proxy(proxy.proxy_url)

                # Формируем словарь с новыми данными для этого прокси
                updated_data = {
                    "proxy_url": proxy.proxy_url,
                    "is_working": is_working,
                    "failed_attempts": proxy.failed_attempts,
                    "is_active": proxy.is_active
                }
                # Кидаем в очередь в памяти
                queue.put_nowait(updated_data)

        # 5. Запускаем параллельную проверку пулом по 50 штук
        tasks = [db_proxy_worker(p) for p in proxies]
        await asyncio.gather(*tasks)

        # 6. ОстанавливаемSaver: шлем сигнал завершения и ждем окончания записи остатков
        await queue.put(None)
        await saver_task

        logging.info("Параллельная перепроверка базы данных успешно завершена.")

        # 5. Смотрим остаток и шлём email при необходимости
        active_count = await ProxyRepository.get_active_count()
        logging.info(f"Проверка базы завершена. Осталось живых прокси: {active_count}")

        if active_count < Config.MIN_PROXIES_LIMIT:
            logging.warning(
                f"Количество прокси ({active_count}) ниже лимита ({Config.MIN_PROXIES_LIMIT})! Отправляю email..."
            )
            try:
                # 1. Скачиваем свежие списки (внутри метода уже зашиты 3 попытки)
                web_proxies = await ProxyService.download_proxies_from_web()
                if web_proxies:
                    logging.info(f"Найдено {len(web_proxies)} прокси в сети. Запускаю их параллельную проверку...")
                    # Прогоняем скачанные с веба прокси через наш стандартный файловый парсер!
                    # Нам не нужно писать новый код, мы просто вызовем наш  метод, передав список в воркеры
                    web_queue = asyncio.Queue()
                    web_working = []

                    # Запускаем фоновый накопитель для вставки в БД
                    web_saver_task = asyncio.create_task(ProxyService.db_saver_consumer(web_queue, web_working))

                    # Проверяем скачанные URL семафором на 20 потоков
                    web_semaphore = asyncio.Semaphore(20)
                    web_tasks = [ProxyService.worker(url, web_semaphore, web_queue) for url in web_proxies]
                    await asyncio.gather(*web_tasks)

                    # Мягко закрываем очередь веба
                    await web_queue.put(None)
                    await web_saver_task

                    # Смотрим, сколько прокси удалось реально залить в базу после веб-парсинга
                    active_count = await ProxyRepository.get_active_count()
                    logging.info(f"Авто-добор завершен. Текущее количество живых прокси в БД: {active_count}")

            except Exception as web_error:
                logging.error(
                    f"🚨 Критический сбой авто-добора прокси (все 3 попытки скачивания провалились): {web_error}")

                # 2. И только если ДАЖЕ ПОСЛЕ веб-парсинга прокси всё равно мало — бьем тревогу на e-mail
            if active_count < Config.MIN_PROXIES_LIMIT:
                logging.critical(
                    f"❌ Авто-добор не помог или упал. В базе по-прежнему мало прокси ({active_count}). Отправляю email...")
                await EmailService.send_alarm_email(active_count)

async def main():
    await init_db_tables()

    scheduler = AsyncIOScheduler()
    # Скрипт проверяет папку на новые файлы
    scheduler.add_job(ProxySchedulerTasks.check_folder_job,
                      'interval',
                      minutes=Config.CHECK_FOLDER_INTERVAL_MINUTES)
    # Скрипт перепроверяет базу данных с интервалом в часах из .env
    scheduler.add_job(
        ProxySchedulerTasks.check_database_proxies_job,
        'interval',
        minutes=Config.CHECK_DB_INTERVAL_MINUTES,
        max_instances=1,
        coalesce=True
    )

    logging.info(
        f"Планировщик запущен. Интервал проверки БД: {Config.CHECK_DB_INTERVAL_MINUTES} м. Лимит: {Config.MIN_PROXIES_LIMIT}")
    scheduler.start()

    # Бесконечный цикл для удержания асинхронного процесса контейнера
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Планировщик остановлен.")
