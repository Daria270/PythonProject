import time
import uuid


from framework.internal.http.account import AccountApi
from framework.internal.http.mail import MailApi
from framework.internal.kafka.producer import Producer



def test_failed_registration(account: AccountApi, mail: MailApi) -> None:
    expected_mail = "string@mail.ru"
    account.register_user(login="string", email=expected_mail, password="string")
    for _ in range(10):
        response = mail.find_message(query=expected_mail)
        if response.json()["total"] > 0:
            raise AssertionError("Email not found")
        time.sleep(1)


def test_success_registration(account: AccountApi, mail: MailApi) -> None:
    base = uuid.uuid4().hex
    account.register_user(login="base", email=f"{base}@mai.ru", password="123123123")
    for _ in range(10):
        response.mail = mail.find_message(query=base)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")


def test_success_registration_with_kafka_producer(mail: MailApi, kafka_producer: Producer) -> None:
    base = uuid.uuid4().hex
    message: dict[str, str] = {
        "login": base,
        "email": f"{base}@mail.ru",
        "password": "123123123"
    }

    # producer = KafkaProducer(
    #     bootstrap_servers=["185.185.143.231:9092"], #ссылка на топик в кафка, важно не ошибться в порте
    #     value_serializer=lambda x: json.dumps(x).encode("utf-8"), #нужен для преобразования сообщения в байты,
    #     acks="all", #нужен для подтверждения отправки сообщения
    #     retries=5, #количесвто попыток переподключения для стабильного соединения
    #     retry_backoff_ms=5000, #время между попытками переподключения в милесекундах
    #     request_timeout_ms=70000, #таймаут запросов, время подтверждения запросов
    #     connections_max_idle_ms=65000, #время простоя соединений
    #     reconnect_backoff_ms=5000, #время между попытками
    #     reconnect_backoff_max_ms=10000 #максимальное время между попытками
    # )
    kafka_producer.send_get(topic="register-events", message=message)
    #topic - название топика в кафке, в value сообщение, value: можно вынести в фикстуру
    for _ in range(10):
        response = mail.find_message(query=base)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")







