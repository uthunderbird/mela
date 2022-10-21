from mela import Mela

app = Mela(__name__)


@app.service("printer")
def printer(body, message):
    print(body)
    return body


if __name__ == '__main__':
    app.run()
