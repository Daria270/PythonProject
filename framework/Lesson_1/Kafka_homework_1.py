from kafka import KafkaProducer
import uuid
import json
import time

def test_success_registration_with_kafka_producer():
    base = uuid.uuid4().hex
    message = {
        "login": base,
        "email": f"{base}@mail.ru",
        "password": "123123123",
    }

    producer = KafkaProducer(
        bootstrap_servers=["185.185.143.231:9092"], #ссылка на топик в кафка, важно не ошибться в порте
        value_serializer=lambda x: json.dumps(x).encode("utf-8"), #нужен для преобразования сообщения в байты,
        acks="all", #нужен для подтверждения отправки сообщения
        retries=5, #количесвто попыток переподключения для стабильного соединения
        retry_backoff_ms=5000, #время между попытками переподключения в милесекундах
        request_timeout_ms=70000, #таймаут запросов, время подтверждения запросов
        connections_max_idle_ms=65000, #время простоя соединений
        reconnect_baсkoff_ms=5000, #время между попытками
        reconnect_baсkoff_max_ms=10000 #максимальное время между попытками
    )

    producer.send(topic="register-events", value=message) #в topic - название топика в кафке, в value сообщение, value: можно вынести в фикстуру
    for _ in range(10):
        response = mail.find_message(query=base)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")