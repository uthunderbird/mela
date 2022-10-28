from datetime import datetime
from logging import Logger
from logging import getLogger

from pydantic import BaseModel
from pydantic import Field

from mela import Mela

app = Mela(__name__)


class Document(BaseModel):

    text: str
    url: str
    likes_count: int = Field(alias='likesCount')
    date: datetime


@app.service("printer")
def printer(doc: Document, logger: Logger):
    logger.info(doc)
    return doc


if __name__ == '__main__':
    app.run()
