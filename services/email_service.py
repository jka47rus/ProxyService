import smtplib
import asyncio
import logging
from email.mime.text import MIMEText
from config.config import Config

class EmailService:

    @staticmethod
    def _send_sync_email(subject: str, body: str):
        """Внутренний синхронный метод отправки SSL-письма."""
        if not Config.SMTP_USER or not Config.SMTP_PASSWORD:
            logging.warning("Email не настроен в .env, отправка отменена.")
            return

        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = Config.SMTP_USER
        msg["To"] = Config.Config.TO_EMAIL if hasattr(Config, 'Config') else Config.TO_EMAIL # небольшая подстраховка по имени

        try:
            # Для порта 465 обычно используется SMTP_SSL
            with smtplib.SMTP_SSL(Config.SMTP_HOST, Config.SMTP_PORT, timeout=10) as server:
                server.login(Config.SMTP_USER, Config.SMTP_PASSWORD)
                server.sendmail(Config.SMTP_USER, [Config.TO_EMAIL], msg.as_string())
            logging.info(f"Письмо успешно отправлено на {Config.TO_EMAIL}")
        except Exception as e:
            logging.error(f"Не удалось отправить email: {e}")

    @classmethod
    async def send_alarm_email(cls, current_count: int):
        """Асинхронный вызов отправки уведомления."""
        subject = "🚨 Внимание: Заканчиваются рабочие прокси!"
        body = (
            f"Привет!\n\n"
            f"В базе данных осталось всего {current_count} рабочих прокси.\n"
            f"Это меньше заданного лимита ({Config.MIN_PROXIES_LIMIT}).\n\n"
            f"Пожалуйста, добавьте новые .txt файлы с прокси в папку '{Config.FOLDER_PATH}'."
        )
        # Запускаем синхронную отправку в отдельном потоке, чтобы не вешать асинхронный чекер
        await asyncio.to_thread(cls._send_sync_email, subject, body)
