from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Конфигурация приложения: переменные окружения + файл .env (frozen)."""

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        frozen=True,
        extra="ignore",
    )

    # Приложение и телеметрия
    DEBUG: bool = True
    SERVICE_NAME: str = "payment-system"
    OTLP_ENDPOINT: str = ""

    # Внешние сервисы — обязательные, задаются через .env/окружение
    DATABASE_URL: str
    RABBITMQ_URL: str

    # Аутентификация и webhook
    API_KEY: str = "dev-api-key"
    WEBHOOK_SIGNING_SECRET: str = "dev-webhook-secret"
    WEBHOOK_TIMEOUT: float = 10.0

    # Outbox-воркер
    OUTBOX_POLL_INTERVAL: float = 1.0
    OUTBOX_BATCH_SIZE: int = 100

    # Платёжный шлюз (симуляция задержки и вероятности успеха)
    GATEWAY_MIN_DELAY: float = 2.0
    GATEWAY_MAX_DELAY: float = 5.0
    GATEWAY_SUCCESS_RATE: float = Field(default=0.9, ge=0.0, le=1.0)


settings = Settings()
