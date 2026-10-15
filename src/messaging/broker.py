from faststream.rabbit import RabbitBroker

from config import settings

# Модульный брокер: создаётся лениво, реальное подключение — при broker.start().
broker = RabbitBroker(settings.RABBITMQ_URL)
