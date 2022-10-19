import asyncio

from mela import Mela
from mela.settings import Settings


app = Mela(__name__)

app.settings = Settings()


@app.service("printer")
def printer(message):
    return message


def default_exception_handler(target_loop, context):
    target_loop.stop()
    raise context['exception']


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(default_exception_handler)
    app.run(loop)
