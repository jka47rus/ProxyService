import logging
from datetime import datetime, timezone
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select, func
from config.database import async_session
from models.proxy import ProxyEntity

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
                            is_active=True
                        )

                        # Добавляем правило ON CONFLICT (UPSERT)
                        # Если такой proxy_url уже есть, обновляем существующую запись
                        stmt = stmt.on_conflict_do_update(
                            index_elements=[ProxyEntity.proxy_url],  # По какому полю уникальность
                            set_={
                                'last_checked': datetime.now(timezone.utc),
                                'is_active': True
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