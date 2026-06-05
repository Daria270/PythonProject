import json
import threading
from types import TracebackType



from kafka import KafkaProducer

from framework.internal.singleton import Singleton


# producer.send(topic="register-events", value=message)
# producer.close()

class Producer(Singleton):
    def __init__(self, bootstrap_servers: list[str] = ["185.185.143.231:9092"]): #так делать нельзя, это потом вынести в кофигурацию
        self._bootstrap_servers = bootstrap_servers
        self._producer: KafkaProducer | None = None
        self._lock: threading.Lock() = threading.Lock()

    def start(self) -> None:
        self._producer = KafkaProducer(
            bootstrap_servers=self._bootstrap_servers,  # ссылка на топик в кафка, важно не ошибться в порте
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),  # нужен для преобразования сообщения в байты,
            acks="all",  # нужен для подтверждения отправки сообщения
            retries=5,  # количесвто попыток переподключения для стабильного соединения
            retry_backoff_ms=5000,  # время между попытками переподключения в милесекундах
            request_timeout_ms=70000,  # таймаут запросов, время подтверждения запросов
            connections_max_idle_ms=65000,  # время простоя соединений
            reconnect_backoff_ms=5000,  # время между попытками
            reconnect_backoff_max_ms=10000  # максимальное время между попытками
        )

    def stop(self) -> None:
        if self._producer:
            self._producer.close()
            self._producer = None


    def send(self, topic: str, message: dict[str, str]) -> None:
        if not self._producer:
            raise RuntimeError("Producer is not started")

        try:
            with self._lock:
                future = self._producer.send(topic, message)
                record_metadata = future.get(timeout=10)
                return record_metadata
        except Exception as e:
            raise RuntimeError(f"Failed to send message to Kafka: {e}")

    # def send_put(self, topic: str, input_data: dict) -> None:
    #     if not self._producer:
    #         raise RuntimeError("Producer is not started")
    #
    #     try:
    #         with self._lock:
    #             future = self._producer.send(topic, value=input_data)
    #             record_metadata = future.get(timeout=10)
    #             return record_metadata
    #     except Exception as e:
    #         raise RuntimeError(f"Failed to send message to Kafka: {e}")

    def __enter__(self) -> "Producer":
        self.start()
        return self

    def __exit__(
            self,
            exc_type: type[BaseException],
            exc_val: BaseException | None,
            exc_tb: TracebackType | None,
    ) -> None:
        self.stop()

