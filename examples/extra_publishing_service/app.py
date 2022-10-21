from pydantic import BaseModel, Field
from mela import Mela
from mela.components import Publisher
from mela.settings import Settings


class Document(BaseModel):
    id_: int = Field(alias='id')


app = Mela(__name__)
app.settings = Settings()

log_publisher: Publisher = app.publisher_instance('log')


@app.service('extra_publishing')
async def logger(body: Document):
    await log_publisher.publish(body)
    return body


if __name__ == '__main__':
    app.run()
