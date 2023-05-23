from datetime import datetime

from pydantic import BaseModel

from mela import Mela
from mela.components import Publisher

app = Mela(__name__)


class Document(BaseModel):
    text: str
    url: str
    date: datetime
    has_images: bool = False


@app.service('archiver')
async def archiver(document: Document, images_downloader: Publisher = 'images-downloader') -> Document:
    # archiving document

    if document.has_images:
        await images_downloader.publish(document)

    return document


if __name__ == '__main__':
    app.run()
