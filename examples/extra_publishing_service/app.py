from datetime import datetime

from pydantic import BaseModel, Field

from mela import Mela
from mela.components import Publisher


class Document(BaseModel):

    text: str
    url: str
    likes_count: int = Field(alias='likesCount')
    date: datetime


app = Mela(__name__)


@app.service('extra_publishing')
async def logger(document: Document, extra_publisher: Publisher = 'log'):
    await extra_publisher.publish(document)
    return document


if __name__ == '__main__':
    app.run()
