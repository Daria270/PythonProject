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
        response = mail.find_message(query=base)
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

    kafka_producer.send(topic="register-events", message=message)
    # topic - название топика в кафке, в value сообщение, value: можно вынести в фикстуру
    for _ in range(10):
        response = mail.find_message(query=base)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")

    # Домашнее задание_1


def test_register_events_error_consumer(account: AccountApi, mail: MailApi, kafka_producer: Producer) -> None:
    base = uuid.uuid4().hex
    login = f"login_{base}"
    email = f"{base}@mail.ru"

    message = {

        "input_data": {
            "login": login,
            "email": email,
            "password": "123123123"
        },
        "error_message": {
            "type": "https://tools.ietf.org/html/rfc7231#section-6.5.1",
            "title": "Validation failed",
            "status": 400,
            "traceId": "00-2bd2ede7c3e4dcf40c4b7a62ac23f448-839ff284720ea656-01",
            "errors": {
                "Email": ["Invalid"]
            }
        },
        "error_type": "unknown"
    }

    kafka_producer.send(topic="register-events", message=message)
    # topic - название топика в кафке, в value сообщение, value: можно вынести в фикстуру

    # Опрос почтового сервера
    email_found = False
    for _ in range(10):
        response = mail.find_message(query=email)
        if response.json()["total"] > 0:
            email_found = True
            break
        time.sleep(1)

    assert not email_found, f"Письмо на адрес {email} не пришло на почтовый сервер!"

    # активацию пользователя с помощью метода PUT /user/activate

    response = account.get_user(login=login)

    # проверка статуса сообщения
    assert response.status_code == 404, f"Пользователь создался, хотя должна была быть ошибка валидации! Статус: {response.status_code}"

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
