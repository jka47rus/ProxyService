import logging
from datetime import datetime, timezone
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select, func
from config.database import async_session
from models.proxy import ProxyEntity
from config.config import Config

class ProxyRepository:

    @staticmethod
    async def save_working_proxies(proxies_list: list[str]) -> None:
        """
        Принимает список строк прокси, массово сохраняет их в БД.
        Если прокси уже существует, обновляет last_checked и ставит is_active = True.
        """
        if not proxies_list:
            return

        # Открываем транзакцию (аналог сессии в Hibernate)
        async with async_session() as session:
            async with session.begin():  # Автоматически сделает commit() в конце или rollback() при ошибке
                try:
                    for url in proxies_list:
                        # Формируем сырой INSERT запрос для PostgreSQL
                        stmt = insert(ProxyEntity).values(
                            proxy_url=url,
                            last_checked=datetime.now(timezone.utc),
                            is_active=True,
                            failed_attempts=0
                        )

                        # Добавляем правило ON CONFLICT (UPSERT)
                        # Если такой proxy_url уже есть, обновляем существующую запись
                        stmt = stmt.on_conflict_do_update(
                            index_elements=[ProxyEntity.proxy_url],  # По какому полю уникальность
                            set_={
                                'last_checked': datetime.now(timezone.utc),
                                'is_active': True,
                                'failed_attempts': 0
                            }
                        )

                        await session.execute(stmt)

                    logging.info(f"Успешно сохранено/обновлено в БД: {len(proxies_list)} прокси.")
                except Exception as e:
                    logging.error(f"Ошибка при сохранении прокси в БД: {e}")

    @staticmethod
    async def get_all_proxies() -> list[ProxyEntity]:
        """Возвращает все прокси из базы данных для проверки."""
        async with async_session() as session:
            stmt = select(ProxyEntity)
            result = await session.execute(stmt)
            return list(result.scalars().all())

    @staticmethod
    async def get_active_count() -> int:
        """Возвращает количество активных (рабочих) прокси в БД."""
        async with async_session() as session:
            stmt = select(func.count()).select_from(ProxyEntity).where(ProxyEntity.failed_attempts < 3)
            result = await session.execute(stmt)
            return result.scalar() or 0

    @staticmethod
    async def update_proxies_batch(batch_data: list[dict]) -> None:
        """Принимает пачку из 10 проверенных прокси и обновляет их в одной транзакции."""
        if not batch_data:
            return

        async with async_session() as session:
            async with session.begin():
                try:
                    for item in batch_data:
                        # Воссоздаем объект, передавая ВСЕ системные поля, чтобы merge прошел корректно
                        proxy_obj = ProxyEntity(
                            proxy_url=item["proxy_url"],
                            last_checked=datetime.now(timezone.utc),
                            is_active=item["is_active"],
                            failed_attempts=item["failed_attempts"]
                        )

                        # Подгружаем объект в контекст транзакции
                        db_proxy = await session.merge(proxy_obj)

                        if item["is_working"]:
                            # Если прокси ответил — обнуляем ошибки и включаем его
                            db_proxy.failed_attempts = 0
                            db_proxy.is_active = True
                        else:
                            # Если не ответил — инкрементируем переданный счетчик
                            db_proxy.failed_attempts += 1
                            db_proxy.is_active = False

                            # Если попыток стало 3 или больше — физически удаляем
                            if db_proxy.failed_attempts >= Config.PROXIES_FAILED_ATTEMPTS:
                                logging.info(f"Удаляем прокси из БД (батч): {db_proxy.proxy_url}")
                                await session.delete(db_proxy)

                    logging.info(f"Батч из {len(batch_data)} результатов проверок успешно сохранен в БД.")
                except Exception as e:
                    logging.error(f"Ошибка при сохранении батча проверок в БД: {e}")

