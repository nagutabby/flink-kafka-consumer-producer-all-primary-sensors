from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction
from pyflink.datastream.window import TimeWindow, TumblingProcessingTimeWindows, SlidingProcessingTimeWindows
from pyflink.common.time import Time
from pyflink.common import Configuration, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema, KafkaSink, KafkaSource, KafkaSourceBuilder, KafkaOffsetsInitializer

from datetime import datetime, timedelta
import re

SECONDARY_SOURCE = 'i483-allsensors'
SECONDARY_SINK = 'i483-fvtt'

config = Configuration()
config.set_integer('parallelism.default', 1)
config.set_integer('taskmanager.numberOfTaskSlots', 2)

env = StreamExecutionEnvironment.get_execution_environment(config)
env.add_jars('file:///Users/nagutabby/flink-1.18.1/opt/flink-sql-connector-kafka-3.1.0-1.18.jar')

class CustomProcessWindowFunction(ProcessWindowFunction[tuple, tuple, str, TimeWindow]):
    def process(self, key, context, elements):
        dict_primary_sources = {}
        for key, message in elements:
            primary_source, _, value = message.split(',')
            value = float(value)
            if primary_source not in dict_primary_sources:
                dict_primary_sources[primary_source] = [value]
            else:
                dict_primary_sources[primary_source].append(value)

        sink_messages = []
        for primary_source, values in dict_primary_sources.items():
            if 'gen-data' not in primary_source:
                primary_sink_average, primary_sink_max, primary_sink_min = get_primary_sink(primary_source)
                average_value = round(sum(values) / len(values), 1)
                max_value = round(max(values), 1)
                min_value = round(min(values), 1)

                sink_messages.append(f'{primary_sink_average},{average_value}')
                sink_messages.append(f'{primary_sink_max},{max_value}')
                sink_messages.append(f'{primary_sink_min},{min_value}')

        return sink_messages

def get_primary_sink(primary_source):
    matched_object = re.match(r'i483-sensors-([a-zA-Z0-9]+)-([a-zA-Z0-9]+)-([a-z0-9_]+)', primary_source)
    entity = matched_object.group(1)
    sensor = matched_object.group(2)
    data_type = matched_object.group(3)

    PRIMARY_SINK_PREFIX = 'i483-sensors-s2410064-analytics'
    primary_sink_average = f'{PRIMARY_SINK_PREFIX}-{entity}_{sensor}_avg-{data_type}'
    primary_sink_max = f'{PRIMARY_SINK_PREFIX}-{entity}_{sensor}_max-{data_type}'
    primary_sink_min = f'{PRIMARY_SINK_PREFIX}-{entity}_{sensor}_min-{data_type}'

    return primary_sink_average, primary_sink_max, primary_sink_min

def create_data_stream_and_sink_back(secondary_source):
    unix_timestamp_five_minutes_ago_s = datetime.timestamp(datetime.now() - timedelta(minutes=5))
    unix_timestamp_five_minutes_ago_ms = round(unix_timestamp_five_minutes_ago_s * 1000)

    kafka_source = KafkaSourceBuilder() \
        .set_starting_offsets(KafkaOffsetsInitializer.timestamp(unix_timestamp_five_minutes_ago_ms)) \
        .set_topics(secondary_source) \
        .set_bootstrap_servers('150.65.230.59:9092') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    data_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.for_monotonous_timestamps(),
        'Kafka source'
    ) \
    .map(lambda x: (0, x), Types.TUPLE([Types.INT(), Types.STRING()])) \
    .key_by(lambda x: x[0]) \
    .window(SlidingProcessingTimeWindows.of(Time.minutes(5), Time.seconds(30))) \

    kafka_producer = FlinkKafkaProducer(
        SECONDARY_SINK,
        SimpleStringSchema(),
        {'bootstrap.servers': '150.65.230.59:9092'},
    )

    data_stream.process(CustomProcessWindowFunction(), Types.STRING()).add_sink(kafka_producer)

create_data_stream_and_sink_back(SECONDARY_SOURCE)

env.execute()
