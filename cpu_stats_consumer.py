from kafka import KafkaConsumer
from config import Config
from dbworker import DBWorker
import json

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
        self._kafka_consumer = self.__connect_kafka_consumer(
            self._config.get_setting("broker", "name"),
            self._config.get_setting("broker", "port")
            )

    def __connect_kafka_consumer(self, server, port):
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
                            self._config.get_setting("messages", "topic"),
                            auto_offset_reset= self._config.get_setting("messages", "auto_offset_reset"),
                            bootstrap_servers=[f"{server}:{port}"],
                            client_id="demo_client",
                            group_id=self._config.get_setting("messages", "group_id"),
                            security_protocol=self._config.get_setting("broker", "security_protocol"),
                            ssl_cafile=self._config.get_setting("broker", "ssl_cafile"),
                            ssl_certfile=self._config.get_setting("broker", "ssl_certfile"),
                            ssl_keyfile=self._config.get_setting("broker", "ssl_keyfile")
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
        # https://help.aiven.io/en/articles/489572-getting-started-with-aiven-kafka
        # Call poll twice. First call will just assign partitions for our
        # consumer without actually returning anything
        for _ in range(2):
            raw_msgs = self._kafka_consumer.poll(timeout_ms=1000)
            for tp, msgs in raw_msgs.items():
                for msg in msgs:
                    print("============")
                    print("Received: {}".format(msg.value))
                    print("============")
                    parsed_records.append(msg.value)

    # Commit offsets so we won't get the same messages again
        self._kafka_consumer.commit()
        self._kafka_consumer.close()

        return parsed_records

    def send_to_db(self, values):
        """
        Sends Values to Database
        It first creates Table if it does not exists and then sends traverses list to send values

        Parameters
        ----------
        values: List of json strings
        """
        worker = DBWorker("db.ini")
        try:
            create_cpustats_table_qry = '''
                CREATE TABLE IF NOT EXISTS cpustats(
                    Category        CHAR(50),
                    SubCategory     CHAR(50),
                    Value           REAL
                );
                '''
            worker.ddl_qry(create_cpustats_table_qry)
            for val in values:
                jval = json.loads(val)
                for key,value in iter(jval.items()):
                    if type(value) is dict:
                        for k, v in iter(value.items()):
                            worker.dml_query(f"INSERT INTO cpustats VALUES('{key}', '{k}', {v})")
                    else:
                        worker.dml_query(f"INSERT INTO cpustats VALUES('{key}', '{key}', {v})")
        except Exception as e:
            print(f"Exception in db operation. Error is -- {e}")
        finally:
            worker.close()

if __name__ == '__main__':
    print('Running Consumer..')
    consumer = CPUStatsConsumer("consumer.ini")
    print("Consuming messages")
    records = consumer.consume_messages()
    if len(records) > 0:
        print("Sending to Database")
        consumer.send_to_db(records)
    else:
        print("No messages found")
