# Alem Mela

## DEPRECATED

Use [FastStream](https://github.com/airtai/faststream) instead.

## Overview

Asynchronous framework that makes it really simple to build RabbitMQ services.

## Installation

`pip install mela`

## Usage

Basic usage does look like this:

`app.py`:
```
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
```

`application.yml`:
```
connections:
  default:
    host: localhost
    port: 5672
    username: user
    password: bitnami

services:
  printer:
    consumer:
      exchange: general-sentiment-x
      routing_key: general-sentiment-q
      queue: general-sentiment-q
    publisher:
      exchange: general-sentiment-x
      routing_key: general-sentiment-q
```


For more use cases check `/examples` directory.

## Contribute

Clone this repo, create virtualenv using `make setup` command.

NOTE: If you use PyCharm - don't let it create its own virtualenv.

Run tests using `make test`

Run linter using `make lint`

Run mypy using `make type`

Feel free to create issues.

Feel free to contribute.

Feel free to add yourself to `Authors` block in this document.

You also can join [our Telegram chat](https://t.me/MelaFramework).

Despite major version, there is a lot of work undone.
You can check TODO section in changelog to find an interesting task 
for you or check issues section of this repository.

## Contributors

Developed in Alem Research.

Core maintainer: Daniyar Supiyev (undead.thunderbird@gmail.com).

Sponsor: Sergazy Narynov.
