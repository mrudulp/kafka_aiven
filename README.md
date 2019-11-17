# kafka_aiven
Sample application using aiven services

This application uses Kafka & Postgresql service from aiven

There are two main python scripts here.
* cpu_stats_producer.py -- This produces cpu statistics that are sent to Kafka service in aiven cloud.
* cpu_stats_consumer.py -- This consumes messages from Kafka service in aiven cloud. This also sends consumed messages to Postgresql database hosted in aiven cloud.

## Running
* Producer can be run `python cpu_stats_producer.py`
* Consumer can be run `python cpu_stats_consumer.py`

## Running Tests
* Goto root of the project and run following commands --
    `python -m pytest -v tests\test_dbworker.py`
    `python -m pytest -v tests\test_kafka.py`

*Note*

* One needs to First fill up relevant information in the ini files.

# REFERENCES

* https://opensource.com/article/18/4/metrics-monitoring-and-python
* https://towardsdatascience.com/getting-started-with-apache-kafka-in-python-604b3250aa05