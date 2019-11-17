import psutil
from time import sleep
from kafka import KafkaProducer
import json
from config import Config


class CPUStatsProducer:

    def __init__(self, *args, **kwargs):
        """
        Initialises Producer

        Parameters
        ----------
        args: Arguments to init. Here INI file name is expected as the only argument

        """
        ini_file = args[0]
        self._config = Config(ini_file)
        self._kafka_producer = self.__connect_kafka_producer(
            self._config.get_setting("broker", "name"),
            self._config.get_setting("broker", "port")
            )

    def __connect_kafka_producer(self, server, port):
        """
        Connect to broker as producer.

        Parameters
        ----------
        server: Server Name
        port: Port where broker is connected.
        """
        _producer = None
        try:
            _producer = KafkaProducer(
                            bootstrap_servers=[f"{server}:{port}"],
                            # https://help.aiven.io/en/articles/489572-getting-started-with-aiven-kafka
                            security_protocol=self._config.get_setting("broker", "security_protocol"),
                            ssl_cafile=self._config.get_setting("broker", "ssl_cafile"),
                            ssl_certfile=self._config.get_setting("broker", "ssl_certfile"),
                            ssl_keyfile=self._config.get_setting("broker", "ssl_keyfile")
                            )
        except Exception as ex:
            print('Exception while connecting Kafka')
            print(str(ex))
        finally:
            return _producer


    def __publish_message(self, topic_name, key, value):
        """
        Publishes Messages

        Parameters
        ----------
        topic_name: Topic where messages are published
        key: Key
        value: Message to be sent to Kafka broker

        """
        try:
            key_bytes = bytes(key, encoding='utf-8')
            value_bytes = bytes(value, encoding='utf-8')
            self._kafka_producer.send(topic_name, key=key_bytes, value=value_bytes)
            self._kafka_producer.flush()
            print('Message published successfully.')
        except Exception as ex:
            print('Exception in publishing message')
            print(str(ex))

    def __construct_cpu_stats_dict(self, cpu_times, cpu_stats, cpu_cnt, cpu_freq):
        """
        Constructs cpu stats that needs to be sent to broker
        Parameters
        ----------
        cpu_times: Value from CPU times
        cpu_stats: Value from CPU stats
        cpu_cnt: Value from CPU count
        cpu_freq: Value from CPU Frequency

        """
        cpu_json = {
            "cpu_times":{
                "user":cpu_times.user,
                "system": cpu_times.system,
                "idle": cpu_times.idle,
                "interrupt": cpu_times.interrupt
            },
            "cpu_stats":{
                "ctx_switches": cpu_stats.ctx_switches,
                "interrupts": cpu_stats.interrupts,
                "soft_interrupts": cpu_stats.soft_interrupts,
                "syscalls": cpu_stats.syscalls
            },
            "cpu_freq":{
                "current": cpu_freq.current,
                "min": cpu_freq.min,
                "max": cpu_freq.max
            },
            "cpu_cnt":cpu_cnt
        }
        return cpu_json

    def __generate_cpu_stats(self):
        """
        Generates cpu stats

        """
        cpu_times = psutil.cpu_times()
        cpu_stats = psutil.cpu_stats()
        cpu_cnt = psutil.cpu_count(logical=True)
        cpu_freq = psutil.cpu_freq()
        cpu_stats_json = self.__construct_cpu_stats_dict(cpu_times, cpu_stats, cpu_cnt, cpu_freq)
        return cpu_stats_json

    def produce_messages(self, msg_cnt=10):
        """
        Publishes messages

        Parameters
        ---------
        msg_cnt: Number of messages to publish
        """
        for i in range(msg_cnt):
            print("=============")
            cpu_stats_json = self.__generate_cpu_stats()
            self.__publish_message(
                                self._config.get_setting("messages", "topic"),
                                self._config.get_setting("messages", "key"),
                                json.dumps(cpu_stats_json)
                                )
            print(cpu_stats_json)
            print("=============")
            sleep(2)

if __name__ == '__main__':
    producer = CPUStatsProducer("producer.ini")
    producer.produce_messages()