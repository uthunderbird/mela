from pydantic import BaseModel
from datetime import datetime

from mela import Mela

app = Mela(__name__)


class Document(BaseModel):
    text: str
    url: str
    date: datetime


@app.service('printer')
def printer(body: Document) -> Document:
    print(body)
    return body


if __name__ == '__main__':
    app.run()