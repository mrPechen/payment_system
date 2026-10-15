import hmac
from typing import Annotated

from fastapi import Header, HTTPException, status

from config import settings


async def require_api_key(
    x_api_key: Annotated[str | None, Header()] = None,
) -> None:
    """
    Проверка API-ключа из заголовка X-API-Key (constant-time сравнение).

    :param x_api_key: значение заголовка X-API-Key
    :raises HTTPException: 401 при отсутствии или несовпадении ключа
    """
    provided = x_api_key or ""
    # hmac.compare_digest устойчив к timing-атакам — обязателен для секретов.
    if not hmac.compare_digest(provided.encode(), settings.API_KEY.encode()):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )
