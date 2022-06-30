from aio_pika import IncomingMessage
from mela import Mela

app = Mela(__name__)
app.read_config_yaml('application.yml')


@app.service("printer")
def printer(body, message: IncomingMessage):
    print(body)
    print(message.routing_key)
    return body


if __name__ == '__main__':
    app.run()
