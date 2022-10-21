from pydantic import BaseModel
from pydantic import Field
from datetime import datetime
from aio_pika import IncomingMessage

from mela import Mela

app = Mela(__name__)


class Document(BaseModel):

    text: str
    url: str
    likes_count: int = Field(alias='likesCount')
    date: datetime


@app.service('printer')
def printer(body: Document, text: str, date: str, message: IncomingMessage, likes_count: str = 0, *, url: str):
    print(body, text, date, message, likes_count, url)
    return body


if __name__ == '__main__':
    app.run()
