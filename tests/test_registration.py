from framework.internal.http.account import AccountApi
from framework.internal.http.mail import MailApi


def test_failed_registration(account: AccountApi, mail: MailApi) -> None:
    expected_mail = "string@mail.ru"
    response = account.register_user(
        login="string", email=expected_mail, password="string"
    )
    response.mail = mail.find_message(query=expected_mail)
