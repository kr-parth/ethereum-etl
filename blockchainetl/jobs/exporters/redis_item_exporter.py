import collections
import json
import logging

import redis

from blockchainetl.jobs.exporters.converters.composite_item_converter import CompositeItemConverter


class RedisItemExporter:

    def __init__(self, output, item_type_to_queue_mapping, converters=()):
        self.item_type_to_queue_mapping = item_type_to_queue_mapping
        self.converter = CompositeItemConverter(converters)
        self.connection_url = self.get_connection_url(output)

        # connection = redis.Redis(url="redis://" + self.connection_url)
        # self.connection = redis.Redis(url="redis://" + self.connection_url)
        self.connection = redis.StrictRedis.from_url("redis:" + self.connection_url)
        # self.channel = connection.channel()

        # for item_type, queue in item_type_to_queue_mapping.items():
        #     self.channel.queue_declare(queue=queue, durable=True)


    def get_connection_url(self, output):
        try:
            return output.split('//')[1]
        except KeyError:
            raise Exception('Invalid redis output param, It should be in format of "redis://<host>:<port>"')

    def open(self):
        pass

    def export_items(self, items):
        for item in items:
            self.export_item(item)

    def export_item(self, item):
        item_type = item.get('type')
        if item_type is not None and item_type in self.item_type_to_queue_mapping:
            data = json.dumps(item).encode('utf-8')
            # logging.info(data)
            return self.connection.publish("chan1", data)
            # return self.channel.basic_publish(exchange='', routing_key=self.item_type_to_queue_mapping[item_type], body=data, properties=pika.BasicProperties(
            #     delivery_mode = pika.spec.PERSISTENT_DELIVERY_MODE
            #     ))
        else:
            logging.warning('Topic for item type "{}" is not configured.'.format(item_type))

    def convert_items(self, items):
        for item in items:
            yield self.converter.convert_item(item)

    def close(self):
        pass


def group_by_item_type(items):
    result = collections.defaultdict(list)
    for item in items:
        result[item.get('type')].append(item)

    return result