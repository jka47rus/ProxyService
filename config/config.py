import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    DATABASE_URL: str = os.getenv("DATABASE_URL")
    FOLDER_PATH: str = os.getenv("FOLDER_PATH")
    CHECK_DB_INTERVAL_MINUTES: int = int(os.getenv("CHECK_DB_INTERVAL_MINUTES", "1"))
    MIN_PROXIES_LIMIT: int = int(os.getenv("MIN_PROXIES_LIMIT", "5"))
    CHECK_FOLDER_INTERVAL_MINUTES: int = int(os.getenv("CHECK_FOLDER_INTERVAL_MINUTES", "2"))
    PROXIES_FAILED_ATTEMPTS: int = int(os.getenv("PROXIES_FAILED_ATTEMPTS", "3"))
    TIME_OF_CHECKING_PROXY_SECONDS: int = int(os.getenv("TIME_OF_CHECKING_PROXY_SECONDS", "5"))

    SMTP_HOST: str = os.getenv("SMTP_HOST")
    SMTP_PORT: int = int(os.getenv("SMTP_PORT", "465"))
    SMTP_USER: str = os.getenv("SMTP_USER")
    SMTP_PASSWORD: str = os.getenv("SMTP_PASSWORD")
    TO_EMAIL: str = os.getenv("TO_EMAIL")