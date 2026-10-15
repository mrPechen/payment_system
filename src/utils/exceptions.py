class NotFoundError(Exception):
    """Сущность с указанным значением не найдена.

    :param value: значение, по которому сущность не найдена
    """

    def __init__(self, value: str) -> None:
        self.value = value
        super().__init__(value)


class AlreadyExistsError(Exception):
    """Сущность с таким значением уже существует.

    :param value: значение, которое уже занято
    """

    def __init__(self, value: str) -> None:
        self.value = value
        super().__init__(value)
