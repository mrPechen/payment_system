import uuid

import aio_pika
import pytest
from faststream.rabbit import RabbitBroker

from src.messaging import publisher, topology

pytestmark = pytest.mark.integration


async def test_retry_message_lands_in_retry_queue(rabbit_url):
    broker = RabbitBroker(rabbit_url)
    async with broker:
        await topology.declare_topology(broker)
        await publisher.publish_retry(broker, uuid.uuid4(), 1)

    conn = await aio_pika.connect_robust(rabbit_url)
    async with conn:
        channel = await conn.channel()
        queue = await channel.declare_queue("payments.retry.1", passive=True)
        message = await queue.get(timeout=10)
        assert message is not None
        assert message.headers["x-retry-count"] == 1
        await message.ack()


async def test_dlq_message_lands_in_dlq(rabbit_url):
    broker = RabbitBroker(rabbit_url)
    async with broker:
        await topology.declare_topology(broker)
        await publisher.publish_dlq(broker, uuid.uuid4(), "reason")

    conn = await aio_pika.connect_robust(rabbit_url)
    async with conn:
        channel = await conn.channel()
        queue = await channel.declare_queue("payments.dlq", passive=True)
        message = await queue.get(timeout=10)
        assert message is not None
        assert message.headers["x-reason"] == "reason"
        await message.ack()
