from datetime import datetime, timezone
from sqlalchemy import String, DateTime, Boolean, Integer
from sqlalchemy.orm import Mapped, mapped_column
from config.database import Base


class ProxyEntity(Base):
    __tablename__ = "proxies"
    __table_args__ = {"schema": "proxy_bot"}

    # @Column(unique = true, nullable = false, length = 255)
    proxy_url: Mapped[str] = mapped_column(String(255), primary_key=True, nullable=False)

    # Время последней проверки (по умолчанию текущее время)
    last_checked: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc))

    # Флаг активности прокси
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    # Счётчик неудачных попыток подряд (по умолчанию 0)
    failed_attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Аналог toString() в Java для удобного логирования
    def __repr__(self) -> str:
        return f"<ProxyEntity(id={self.id}, url={self.proxy_url}, fails={self.failed_attempts})>"
