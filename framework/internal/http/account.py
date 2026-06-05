import httpx


class AccountApi:
    def __init__(self, base_url: str = "http://185.185.143.231:8085") -> None:
        self._base_url = base_url
        self._client = httpx.Client(base_url=self._base_url)

    def register_user(self, login: str, email:str, password: str) -> httpx.Response:
        data = {
            "login": login,
            "email": email,
            "password": password
        }
        response = self._client.post("/register/user/async-register", json=data)
        print(response.content)
        return response

    # def activate_user(self, login: str, email: str) -> httpx.Response:
    #     data = {
    #         "login": login,
    #         "email": email
    #     }
    #     response = self._client.put("/user/activate", json=data)
    #     return response

    def get_user(self, login: str | None = None, email: str | None = None) -> httpx.Response:
        if login:
            return self._client.get(f"/user/{login}")
        elif email:
            return self._client.get("/user/activate", params={"email": email})
        else:
            raise ValueError("Нужно указать либо login, либо email")


