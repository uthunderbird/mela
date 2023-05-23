from datetime import datetime
from datetime import timedelta
from pydantic import BaseModel

from mela import IncomingMessage
from mela import Mela
from mela.components.exceptions import NackMessageError

app = Mela(__name__)


class Document(BaseModel):
    text: str
    url: str
    date: datetime


@app.service("filter")
async def filter_(body: Document, message: IncomingMessage):
    if body.date > datetime.utcnow():
        # First way: we can raise special exception with some `requeue` value
        raise NackMessageError("We are not working with time travellers", requeue=False)
    elif body.date < datetime.utcnow() - timedelta(days=365):
        # Second way: we can manually nack message via IncomingMessage object
        # As you can see, in this case we can't write any message about requeue reason.
        # But it is still useful if you need to silently send message to DLX
        await message.nack(requeue=False)  # Go to archive, dude

    if body.url == '':
        # Third way: we can raise almost any exception. It should be or should not be requeued
        # based on `requeue_broken_messages` value
        raise AssertionError("Message without url is not acceptable")

    return body


if __name__ == '__main__':
    app.run()
