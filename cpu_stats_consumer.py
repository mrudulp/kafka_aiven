from kafka import KafkaConsumer
from config import Config

class CPUStatsConsumer:

    def __init__(self, *args, **kwargs):
        """
        Initialises Consumer

        Parameters
        ----------
        args: Arguments to init. Here INI file name is expected as the only argument

        """
        ini_file = args[0]
        self._config = Config(ini_file)
        self._kafka_consumer = self.connect_kafka_consumer(
            self._config.get_broker_setting("name"),
            self._config.get_broker_setting("port")
            )

    def connect_kafka_consumer(self, server, port):
        """
        Connects as a Kafka consumer

        Parameters
        ----------
        server: Name of the server
        port  : Port where server is running
        """
        _consumer = None
        try:
            _consumer = KafkaConsumer(
                            self._config.get_messages_setting("topic"),
                            auto_offset_reset= self._config.get_messages_setting("auto_offset_reset"),
                            bootstrap_servers=[f"{server}:{port}"],
                            api_version=(
                                int(self._config.get_broker_setting("api_min")),
                                int(self._config.get_broker_setting("api_max"))
                                ),
                            consumer_timeout_ms=int(self._config.get_messages_setting("timeout")),
                            enable_auto_commit=self._config.get_messages_setting("enable_auto_commit"),
                            auto_commit_interval_ms=int(self._config.get_messages_setting("commit_interval")),
                            group_id=self._config.get_messages_setting("group_id")
                            )
        except Exception as ex:
            print('Exception while connecting Kafka')
            print(str(ex))
        finally:
            return _consumer

    def consume_messages(self):
        """
        Consumes All messages under the topic.
        """
        parsed_records = []
        for msg in self._kafka_consumer:
            print(msg.value)
            parsed_records.append(msg.value)
        self._kafka_consumer.close()

if __name__ == '__main__':
    print('Running Consumer..')
    consumer = CPUStatsConsumer("consumer.ini")
    consumer.consume_messages()

