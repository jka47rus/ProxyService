import asyncio
import logging
from datetime import datetime, timezone
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from config.config import Config
from config.database import async_session, engine, Base
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
        """Задача 1: Поиск новых файлов в папке (каждые 30 сек)."""
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

        async with async_session() as session:
            async with session.begin():
                for proxy in proxies:
                    # Используем ваш метод проверки из ProxyService
                    is_working = await ProxyService.check_proxy(proxy.proxy_url)

                    if is_working:
                        # Если работает — обнуляем счётчик ошибок
                        if proxy.failed_attempts > 0:
                            logging.info(f"Прокси ожил, обнуляем счётчик ошибок: {proxy.proxy_url}")
                            proxy.failed_attempts = 0
                        proxy.last_checked = datetime.now(timezone.utc)
                        proxy.is_active = True
                    else:
                        # Если не работает — инкрементим счётчик
                        proxy.failed_attempts += 1
                        proxy.last_checked = datetime.now(timezone.utc)
                        logging.warning(f"Прокси не ответил ({proxy.failed_attempts}/{Config.PROXIES_FAILED_ATTEMPTS}): {proxy.proxy_url}")

                        # Если попыток стало 3 или больше — удаляем из базы физически
                        if proxy.failed_attempts >= Config.PROXIES_FAILED_ATTEMPTS:
                            logging.danger = logging.error  # просто логируем жесткую ошибку
                            logging.error(f"Удаляем прокси из БД ({Config.PROXIES_FAILED_ATTEMPTS} неудачные проверки подряд): {proxy.proxy_url}")
                            await session.delete(proxy)
                        else:
                            # Временно помечаем неактивным, но не удаляем
                            proxy.is_active = False

        # После проверки всех прокси и завершения транзакции смотрим остаток
        active_count = await ProxyRepository.get_active_count()
        logging.info(f"Проверка базы завершена. Осталось живых прокси: {active_count}")

        # Если прокси мало — шлём письмо
        if active_count < Config.MIN_PROXIES_LIMIT:
            logging.warning(
                f"Количество прокси ({active_count}) ниже лимита ({Config.MIN_PROXIES_LIMIT})! Отправляю email...")
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
