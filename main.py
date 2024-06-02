from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction
from pyflink.datastream.window import TimeWindow, TumblingProcessingTimeWindows, SlidingProcessingTimeWindows
from pyflink.common.time import Time
from pyflink.common import Configuration, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema, KafkaSink, KafkaSource, KafkaSourceBuilder, KafkaOffsetsInitializer

from datetime import datetime, timedelta, timezone, time

TOPIC_TEAM_PREFIX = "i483-sensors-team2"
TOPIC_INDIVIDUAL_PREFIX = "i483-sensors-s2410064"

class AverageProcessWindowFunction(ProcessWindowFunction[tuple, tuple, str, TimeWindow]):
    def process(self, key, context, elements):
        list_value = []
        for element in elements:
            list_value.append(element[1])

        average_value = round(sum(list_value) / len(list_value), 1)

        return [str(average_value)]

class MaxProcessWindowFunction(ProcessWindowFunction[tuple, tuple, str, TimeWindow]):
    def process(self, key, context, elements):
        list_value = []
        for element in elements:
            list_value.append(element[1])

        max_value = round(max(list_value), 1)

        return [str(max_value)]


class MinProcessWindowFunction(ProcessWindowFunction[tuple, tuple, str, TimeWindow]):
    def process(self, key, context, elements):
        list_value = []
        for element in elements:
            list_value.append(element[1])

        min_value = round(min(list_value), 1)

        return [str(min_value)]

config = Configuration()
config.set_integer('parallelism.default', 1)
config.set_integer('taskmanager.numberOfTaskSlots', 2)

env = StreamExecutionEnvironment.get_execution_environment(config)
env.add_jars('file:///Users/nagutabby/flink-1.19.0/opt/flink-sql-connector-kafka-3.1.0-1.18.jar')

def create_data_stream_and_sink_back(topic):
    unix_timestamp_five_minutes_ago_s = datetime.timestamp(datetime.now() - timedelta(minutes=5))
    unix_timestamp_five_minutes_ago_ms = round(unix_timestamp_five_minutes_ago_s * 1000)

    kafka_source = KafkaSourceBuilder() \
        .set_starting_offsets(KafkaOffsetsInitializer.timestamp(unix_timestamp_five_minutes_ago_ms)) \
        .set_topics(topic) \
        .set_bootstrap_servers('150.65.230.59:9092') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    data_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.for_monotonous_timestamps(),
        'Kafka source'
    ) \
    .map(lambda x: (0, float(x)), output_type=Types.TUPLE([Types.INT(), Types.FLOAT()])) \
    .key_by(lambda x: x[0]) \
    .window(SlidingProcessingTimeWindows.of(Time.minutes(5), Time.seconds(30))) \

    kafka_producer = get_kafka_producer(topic, 'average')
    data_stream.process(AverageProcessWindowFunction(), Types.STRING()).add_sink(kafka_producer)

    kafka_producer = get_kafka_producer(topic, 'max')
    data_stream.process(MaxProcessWindowFunction(), Types.STRING()).add_sink(kafka_producer)

    kafka_producer = get_kafka_producer(topic, 'min')
    data_stream.process(MinProcessWindowFunction(), Types.STRING()).add_sink(kafka_producer)

def get_kafka_producer(source, metrics):
    if metrics == 'average':
        if 's2410064' in source:
            if 'SCD41' in source:
                if 'co2' in source:
                    topic = f'{TOPIC_INDIVIDUAL_PREFIX}-analytics-s2410064_SCD41_avg-co2'
                elif 'humidity' in source:
                    topic = f'{TOPIC_INDIVIDUAL_PREFIX}-analytics-s2410064_SCD41_avg-humidity'
                elif 'temperature' in source:
                    topic = f'{TOPIC_INDIVIDUAL_PREFIX}-analytics-s2410064_SCD41_avg-temperature'
            elif 'BMP180' in source:
                if 'temperature' in source:
                    topic = f'{TOPIC_INDIVIDUAL_PREFIX}-analytics-s2410064_BMP180_avg-temperature'
                elif 'air_pressure' in source:
                    topic = f'{TOPIC_INDIVIDUAL_PREFIX}-analytics-s2410064_BMP180_avg-air_pressure'
        elif 'team2' in source:
            if 'SCD41' in source:
                if 'co2' in source:
                    topic = f'{TOPIC_TEAM_PREFIX}-analytics-team2_SCD41_avg-co2'
                elif 'humidity' in source:
                    topic = f'{TOPIC_TEAM_PREFIX}-analytics-team2_SCD41_avg-humidity'
                elif 'temperature' in source:
                    topic = f'{TOPIC_TEAM_PREFIX}-analytics-team2_SCD41_avg-temperature'
            elif 'BMP180' in source:
                if 'temperature' in source:
                    topic = f'{TOPIC_TEAM_PREFIX}-analytics-team2_BMP180_avg-temperature'
                elif 'air_pressure' in source:
                    topic = f'{TOPIC_TEAM_PREFIX}-analytics-team2_BMP180_avg-air_pressure'
            elif 'BH1750' in source:
                topic = f'{TOPIC_TEAM_PREFIX}-analytics-team2_BH1750_avg-illumination'
            elif 'RPR0521RS' in source:
                topic = f'{TOPIC_TEAM_PREFIX}-analytics-team2_RPR0521RS_avg-ambient_illumination'
    elif metrics == 'max':
        if 's2410064' in source:
            if 'SCD41' in source:
                if 'co2' in source:
                    topic = f'{TOPIC_INDIVIDUAL_PREFIX}-analytics-s2410064_SCD41_max-co2'
                elif 'humidity' in source:
                    topic = f'{TOPIC_INDIVIDUAL_PREFIX}-analytics-s2410064_SCD41_max-humidity'
                elif 'temperature' in source:
                    topic = f'{TOPIC_INDIVIDUAL_PREFIX}-analytics-s2410064_SCD41_max-temperature'
            elif 'BMP180' in source:
                if 'temperature' in source:
                    topic = f'{TOPIC_INDIVIDUAL_PREFIX}-analytics-s2410064_BMP180_max-temperature'
                elif 'air_pressure' in source:
                    topic = f'{TOPIC_INDIVIDUAL_PREFIX}-analytics-s2410064_BMP180_max-air_pressure'
        elif 'team2' in source:
            if 'SCD41' in source:
                if 'co2' in source:
                    topic = f'{TOPIC_TEAM_PREFIX}-analytics-team2_SCD41_max-co2'
                elif 'humidity' in source:
                    topic = f'{TOPIC_TEAM_PREFIX}-analytics-team2_SCD41_max-humidity'
                elif 'temperature' in source:
                    topic = f'{TOPIC_TEAM_PREFIX}-analytics-team2_SCD41_max-temperature'
            elif 'BMP180' in source:
                if 'temperature' in source:
                    topic = f'{TOPIC_TEAM_PREFIX}-analytics-team2_BMP180_max-temperature'
                elif 'air_pressure' in source:
                    topic = f'{TOPIC_TEAM_PREFIX}-analytics-team2_BMP180_max-air_pressure'
            elif 'BH1750' in source:
                topic = f'{TOPIC_TEAM_PREFIX}-analytics-team2_BH1750_max-illumination'
            elif 'RPR0521RS' in source:
                topic = f'{TOPIC_TEAM_PREFIX}-analytics-team2_RPR0521RS_max-ambient_illumination'
    elif metrics == 'min':
        if 's2410064' in source:
            if 'SCD41' in source:
                if 'co2' in source:
                    topic = f'{TOPIC_INDIVIDUAL_PREFIX}-analytics-s2410064_SCD41_min-co2'
                elif 'humidity' in source:
                    topic = f'{TOPIC_INDIVIDUAL_PREFIX}-analytics-s2410064_SCD41_min-humidity'
                elif 'temperature' in source:
                    topic = f'{TOPIC_INDIVIDUAL_PREFIX}-analytics-s2410064_SCD41_min-temperature'
            elif 'BMP180' in source:
                if 'temperature' in source:
                    topic = f'{TOPIC_INDIVIDUAL_PREFIX}-analytics-s2410064_BMP180_min-temperature'
                elif 'air_pressure' in source:
                    topic = f'{TOPIC_INDIVIDUAL_PREFIX}-analytics-s2410064_BMP180_min-air_pressure'
        elif 'team2' in source:
            if 'SCD41' in source:
                if 'co2' in source:
                    topic = f'{TOPIC_TEAM_PREFIX}-analytics-team2_SCD41_min-co2'
                elif 'humidity' in source:
                    topic = f'{TOPIC_TEAM_PREFIX}-analytics-team2_SCD41_min-humidity'
                elif 'temperature' in source:
                    topic = f'{TOPIC_TEAM_PREFIX}-analytics-team2_SCD41_min-temperature'
            elif 'BMP180' in source:
                if 'temperature' in source:
                    topic = f'{TOPIC_TEAM_PREFIX}-analytics-team2_BMP180_min-temperature'
                elif 'air_pressure' in source:
                    topic = f'{TOPIC_TEAM_PREFIX}-analytics-team2_BMP180_min-air_pressure'
            elif 'BH1750' in source:
                topic = f'{TOPIC_TEAM_PREFIX}-analytics-team2_BH1750_min-illumination'
            elif 'RPR0521RS' in source:
                topic = f'{TOPIC_TEAM_PREFIX}-analytics-team2_RPR0521RS_min-ambient_illumination'

    print(topic)
    kafka_producer = FlinkKafkaProducer(
        topic,
        SimpleStringSchema(),
        {'bootstrap.servers': '150.65.230.59:9092'}
    )

    kafka_producer.set_write_timestamp_to_kafka(True)

    return kafka_producer

create_data_stream_and_sink_back(f'{TOPIC_INDIVIDUAL_PREFIX}-SCD41-co2')
create_data_stream_and_sink_back(f'{TOPIC_INDIVIDUAL_PREFIX}-SCD41-humidity')
create_data_stream_and_sink_back(f'{TOPIC_INDIVIDUAL_PREFIX}-SCD41-temperature')
create_data_stream_and_sink_back(f'{TOPIC_INDIVIDUAL_PREFIX}-BMP180-temperature')
create_data_stream_and_sink_back(f'{TOPIC_INDIVIDUAL_PREFIX}-BMP180-air_pressure')

create_data_stream_and_sink_back(f'{TOPIC_TEAM_PREFIX}-SCD41-co2')
create_data_stream_and_sink_back(f'{TOPIC_TEAM_PREFIX}-SCD41-humidity')
create_data_stream_and_sink_back(f'{TOPIC_TEAM_PREFIX}-SCD41-temperature')
create_data_stream_and_sink_back(f'{TOPIC_TEAM_PREFIX}-BMP180-temperature')
create_data_stream_and_sink_back(f'{TOPIC_TEAM_PREFIX}-BMP180-air_pressure')
create_data_stream_and_sink_back(f'{TOPIC_TEAM_PREFIX}-BH1750-illumination')
create_data_stream_and_sink_back(f'{TOPIC_TEAM_PREFIX}-RPR0521RS-ambient_illumination')

env.execute()
