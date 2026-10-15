from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

MAX_ATTEMPTS = 3

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
DLX_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

NEW_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key="new",
    arguments={
        "x-dead-letter-exchange": "payments.dlx",
        "x-dead-letter-routing-key": "dlq",
    },
)
RETRY_QUEUES = (
    RabbitQueue(
        "payments.retry.1",
        durable=True,
        routing_key="retry.1",
        arguments={
            "x-message-ttl": 2000,
            "x-dead-letter-exchange": "payments",
            "x-dead-letter-routing-key": "new",
        },
    ),
    RabbitQueue(
        "payments.retry.2",
        durable=True,
        routing_key="retry.2",
        arguments={
            "x-message-ttl": 4000,
            "x-dead-letter-exchange": "payments",
            "x-dead-letter-routing-key": "new",
        },
    ),
)
DLQ = RabbitQueue("payments.dlq", durable=True, routing_key="dlq")


async def _declare_bound_queue(
    broker: RabbitBroker, queue: RabbitQueue, exchange: RabbitExchange
) -> None:
    # FastStream 0.7 объявляет очередь и обмен раздельно, поэтому привязку
    # очереди к обмену выполняем вручную (как это делает subscriber внутри).
    q = await broker.declare_queue(queue)
    e = await broker.declare_exchange(exchange)
    await q.bind(e, routing_key=queue.routing())


async def declare_topology(broker: RabbitBroker) -> None:
    await broker.declare_exchange(PAYMENTS_EXCHANGE)
    await broker.declare_exchange(DLX_EXCHANGE)
    await _declare_bound_queue(broker, NEW_QUEUE, PAYMENTS_EXCHANGE)
    for queue in RETRY_QUEUES:
        await _declare_bound_queue(broker, queue, PAYMENTS_EXCHANGE)
    await _declare_bound_queue(broker, DLQ, DLX_EXCHANGE)
