from faststream.rabbit import RabbitBroker

from app.config import get_settings

broker = RabbitBroker(get_settings().rabbitmq_url)
