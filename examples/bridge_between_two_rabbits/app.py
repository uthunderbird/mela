from mela import Mela

app = Mela(__name__)


@app.service("input", "output")
def serve(body, message):
    return body


if __name__ == '__main__':
    app.run()
