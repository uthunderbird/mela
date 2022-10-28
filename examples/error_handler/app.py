from mela import Mela

app = Mela(__name__)


@app.service("splitter")
async def printer(body, message):
    raise NotImplementedError("Method is not implemented")


if __name__ == '__main__':
    app.run()
