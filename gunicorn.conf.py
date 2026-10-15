bind = "0.0.0.0:8000"
workers = 10
worker_class = "uvicorn.workers.UvicornWorker"
timeout = 120
graceful_timeout = 30  # Время на flush телеметрии при остановке
