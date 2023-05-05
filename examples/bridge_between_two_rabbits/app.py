import asyncio

from mela import Mela
from mela.dsl.registries import ConnectionRegistry
from mela.dsl.registries import ExchangeRegistry
from mela.dsl.registries import QueueRegistry
from mela.dsl.registries import BindingRegistry
from mela.dsl.registries import ConsumerRegistry
from mela.dsl.registries import PublisherRegistry
from mela.dsl.registries import ServiceRegistry
from mela.dsl.schema import Schema

app = Mela(__name__)
# app.parse_yaml("application.yaml", validate=True)

schema = Schema('test')

connections = ConnectionRegistry()

input_connection_definition = schema.connections.define(
    name='input',
    url='amqp://guest:guest@localhost:5672/',
)
output_connection_ref_definition = schema.connections.define(
    ref='output_connection',
)
input_exchange_definition = schema.exchanges.define(
    connection=input_connection_definition,
    name='input_exchange',
    type='direct',
)
input_queue_definition = schema.queues.define(
    connection=input_connection_definition,
    name='input_queue',
)
input_binding_definition = schema.bindings.define(
    connection=input_connection_definition,
    exchange=input_exchange_definition,
    queue=input_queue_definition,
    routing_key='yolo',
)
input_consumer_definition = schema.consumers.define(
    connection=input_connection_definition,
    queue=input_queue_definition,
    exchange=input_exchange_definition,
    bindings=[input_binding_definition],
    prefetch_count=1,
)

output_exchange_definition = schema.exchanges.define(
    connection=output_connection_ref_definition,
    name='output_exchange',
    type='direct',
)

output_producer_definition = schema.producers.define(
    connection=output_connection_ref_definition,
    exchange=output_exchange_definition,
    default_routing_key='yolo',
)

bridge_service_definition = schema.services.define(
    name='bridge',
    consumer=input_consumer_definition,
    producer=output_producer_definition,
)


async def serve(body):
    print(body)
    return body

# TODO tests for input and output serialization and deserialization
bridge_service_definition.make_processor(
    serve,
    input_deserialization_map={'body': ['json']},
    output_serializer='json',
)

# TODO tests for validation
schema.validate()

output_connection_definition = app.connections.define(
    name='output_connection',
    host='localhost',
    port=5673,
    username='guest',
    password='guest',
)

loop = asyncio.get_event_loop()

# TODO test references can't be used in global definitions
app.register_schema(schema)

# TODO test references are solved
app.interpreter.validate(schema)


async def main():
    output_connection = await app.interpreter.resolve_connection(output_connection_definition)
    input_connection = await app.interpreter.resolve_connection(input_connection_definition)
    output_connection_by_ref = await app.interpreter.resolve_connection(output_connection_ref_definition)

    assert output_connection is output_connection_by_ref

    schema_declaration = app.interpreter.make_declaration(schema)

    await app.declare(schema_declaration)
    await app.interpreter.apply(schema)
    assert app.services['test.bridge'].consumer.queue.name == 'input_queue'
    assert app.services['test.bridge'].direct_call({'b': 1})['b'] == 1

    app.run(loop)


if __name__ == '__main__':
    loop.run_until_complete(main())
