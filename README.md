# Alem Mela

## Overview

Asynchronous framework that makes it really simple to build RabbitMQ services.

## Installation

`pip install mela`

## Usage

Basic usage does look like this:

`app.py`:
```
from mela import Mela

app = Mela(__name__)
app.read_config_yaml('application.yml')


@app.service("printer")
def printer(body, message):
    # Just print message body and push 
    # unchanged message to output queue.
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


For more use cases and examples please 
check `/examples` directory.

## Authors

Developed in Alem Research.

Core maintainer: Daniyar Supiyev (undead.thunderbird@gmail.com).

Producer: Sergazy Narynov.

