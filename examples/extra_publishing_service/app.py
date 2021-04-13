from mela import Mela

app = Mela(__name__)
app.read_config_yaml('application.yml')

logging_publisher = app.publisher()


SPLITTER_SERVICE_NAME = "splitter"


@app.service(SPLITTER_SERVICE_NAME)
async def logger(body, message):
    default_routing_key = app.services['splitter'].config['publisher']['routing_key']
    i = 0
    for obj in body:
        if i % 2 == 0:
            routing_key = default_routing_key
        else:
            routing_key = "test_queue2"
        yield obj, {'routing_key': routing_key}
        # Anyway, we should publish message into logging exchange
        await logging_publisher.publish(obj, routing_key='.'.join([SPLITTER_SERVICE_NAME, routing_key]))
        i += 1


if __name__ == '__main__':
    app.run()
