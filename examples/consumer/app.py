from pydantic import BaseModel
from pydantic import EmailStr
from mela import Mela

app = Mela(__name__)


class EmailNotification(BaseModel):
    template_name: str
    vars: dict
    receiver: EmailStr


@app.consumer("email-sender")
def printer(body: EmailNotification):
    # Some Jinja2 and SMTP integration
    pass


if __name__ == '__main__':
    app.run()
