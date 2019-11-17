import pytest
import json

from cpu_stats_consumer import CPUStatsConsumer

def test_consumer(produce_messages_single):

    consumer = CPUStatsConsumer("consumer.ini")
    # Assuming Consumer exists and there are messages on the
    records = consumer.consume_messages()
    assert len(records) == 1
    for val in records:
        jval = json.loads(val)
        for key,value in iter(jval.items()):
            print(f"Key is ::{key}")
            assert key in  ("cpu_times", "cpu_stats", "cpu_cnt", "cpu_freq")
