from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from config.config import Config

# Создаем движок. echo=False отключает логирование каждого SQL-запроса в консоль (как show-sql=false)
engine = create_async_engine(Config.DATABASE_URL, echo=False)

# Фабрика сессий. expire_on_commit=False предотвращает детач сущностей после коммита (как в JPA)
async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# Базовый класс для всех будущих сущностей (аналог @MappedSuperclass)
class Base(DeclarativeBase):
    pass