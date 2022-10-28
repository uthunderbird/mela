from mela import Mela
from mela.components.exceptions import NackMessageError

app = Mela(__name__)

i = 0


@app.service("service_with_dlx")
async def printer(body, message):
    global i
    i += 1
    if i % 2 == 0:
        print(body, "NO")
        raise NackMessageError("Method is not implemented", requeue=False)
    else:
        print(body, "YES")
    return body


if __name__ == '__main__':
    app.run()
