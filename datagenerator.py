import logging.config
import random
import string
import threading
import weakref
from collections import defaultdict
from CommonUtil import constants, util
from couchbase_ops.bucketops import BucketOps
from schemagenerator import SchemaGenerator
import copy
from datetime import datetime
import time
import concurrent

class DataGenerator:

    def __init__(self, schema):
        self.num_docs = schema["num_docs"]
        self.doc_key_length = schema["doc_key_length"]
        self.schema_fields = schema["fields"]
        self.docs = {}
        self._lock_docs = threading.Lock()
        self.schema = schema

    def get_docs(self, id, random_key=True, key_to_upsert=None):

        json_doc = {}
        doc = {}
        skip_field = False

        if key_to_upsert:
            doc_key = key_to_upsert
        elif random_key:
            doc_key = ''.join(random.choice(string.printable + '!@#$%^&*()_') for _ in range(self.doc_key_length))
        else:
            doc_key = "customer" + str(id)

        json_doc[doc_key] = {}

        #print(use_predefined_value)

        for field in self.schema_fields:
            if field["can_aggregate"]:
                field_value = random.choice(field["predefined_values"])
            else:
                field_data_type = field["field_data_type"]
                field_value = ""
                if field_data_type.lower() == "boolean":
                    field_value = random.choice([True, False])
                elif field_data_type.lower() == "alphanumeric":
                    field_value = ''.join(
                        random.choice(string.ascii_letters + string.digits) for _ in range(field["field_value_length"]))
                elif field_data_type.lower() == "integer":
                    range_start = 10 ** (field["field_value_length"] - 1)
                    range_end = (10 ** field["field_value_length"]) - 1
                    field_value = random.randint(range_start, range_end)
                elif field_data_type.lower() == "float":
                    precision = random.randint(1, 8)
                    range_start = 10 ** (field["field_value_length"] - 1)
                    range_end = (10 ** field["field_value_length"]) - 1
                    field_value = round(random.uniform(range_start, range_end), precision)
                elif field_data_type.lower() == "letters":
                    field_value = ''.join(random.choice(string.ascii_letters) for _ in range(field["field_value_length"]))
                elif field_data_type.lower() == "string":
                    field_value = ''.join(random.choice(string.printable + '!@#$%^&*()_') for _ in
                                          range(field["field_value_length"]))
                elif field_data_type.lower() == "spl_chars":
                    field_value = ''.join(random.choice(string.whitespace + string.punctuation + '!@#$%^&*()_') for _ in
                                          range(field["field_value_length"]))
                # elif field_data_type.lower() == "date":
                elif field_data_type.lower() == "null":
                    field_value = None
                elif field_data_type.lower() == "missing":
                    field_value = ''.join(random.choice(string.ascii_letters) for _ in range(field["field_value_length"]))
                    skip_field = random.choice([True, False])
                else:
                    logging.info("unknown data type")

            if not skip_field:
                doc[field["field_name"]] = field_value

        with self._lock_docs:
            self.docs[doc_key] = doc

    def generate_docs(self, start, random_key, docs_to_upsert=None):
        with concurrent.futures.ThreadPoolExecutor(max_workers=10, thread_name_prefix="doc-thread") as executor:
            if not docs_to_upsert:
                for index in range(int(self.num_docs)):
                    executor.submit(self.get_docs, start + index, random_key)
            else:
                for index, doc_key in zip(range(int(self.num_docs)), docs_to_upsert.keys()):
                    executor.submit(self.get_docs, start + index, random_key, doc_key)

            executor.shutdown(wait=True)

        return self.docs


class KeepRefs(object):
    __refs__ = defaultdict(list)

    def __init__(self):
        self.__refs__[self.__class__].append(weakref.ref(self)())

    @classmethod
    def get_random_instance(cls):
        return random.choice(cls.__refs__[cls])

    @classmethod
    def get_instances(cls):
        for ins in cls.__refs__[cls]:
            yield ins


class Batch(KeepRefs):
    def __init__(self, start, end, batch_meta):
        super(Batch, self).__init__()
        self.start = start
        self.end = end
        self.batch_meta = batch_meta
        self.items = None
        self.log = logging.getLogger()
        self.bucket_ops = BucketOps()
        self.docs_to_upsert = None
        self.batch_size = self.batch_meta["schema"]["num_docs"]

    def gen_docs(self, docs_to_upsert=None):
        data_gen = DataGenerator(self.batch_meta["schema"])
        if docs_to_upsert:
            self.docs_to_upsert = data_gen.generate_docs(self.start, self.batch_meta["random_key"], docs_to_upsert)
        else:
            self.items = data_gen.generate_docs(self.start, self.batch_meta["random_key"], docs_to_upsert)

    def print_batch(self):
        self.log.info("start {0}, end {1}, num of items {2}".format(self.start, self.end, len(self.items)))

    def batch_ops(self):
        try:
            self.insert_batch()
        except Exception as e:
            logging.critical("Failed inserting docs : " + str(len(str(e).split(","))))


        try:
            docs_to_upsert = self.get_docs_for_ops("UPSERT")
            self.log.info("Upserting docs : {0}".format(len(docs_to_upsert)))
            self.upsert_batch(docs_to_upsert)
        except Exception as e:
            logging.critical("Failed upserting docs : " + str(len(str(e).split(","))))

        try:
            docs_to_delete = self.get_docs_for_ops("DELETE")
            self.log.info("deleting docs : {0}".format(len(docs_to_delete)))
            self.delete_batch(docs_to_delete)
        except Exception as e:
            logging.critical("Failed deleting docs : " + str(len(str(e).split(","))))

    def get_docs_for_ops(self, ops):
        num_docs = int(self.batch_meta[ops]["DOCS"] * self.batch_size)
        tmp_items = {}

        if self.batch_meta[ops]["RANDOM"]:
            for i in range(num_docs):
                random_key = random.choice(list(self.items.keys()))
                tmp_items[random_key] = self.items[random_key]
        else:
            tmp_items = copy.deepcopy(self.items)
            tmp_items = dict(list(tmp_items.items())[:num_docs])

        return tmp_items

    def delete_batch(self, docs_to_delete):

        self.bucket_ops.create_connection(constants.BUCKET_NAME)
        self.bucket_ops.delete_items(docs_to_delete.keys())
        self.bucket_ops.close_connection()

    def upsert_batch(self, docs_to_upsert):
        self.gen_docs(docs_to_upsert)
        self.print_batch()
        self.bucket_ops.create_connection(constants.BUCKET_NAME)
        self.bucket_ops.upsert_items(self.docs_to_upsert)
        self.bucket_ops.close_connection()

    def insert_batch(self):
        num_docs_with_expiry = int(self.batch_meta["EXPIRY"]["DOCS"] * self.batch_size)
        expiry_duration = self.batch_meta["EXPIRY"]["TIME"]

        self.gen_docs()
        self.print_batch()
        self.bucket_ops.create_connection(constants.BUCKET_NAME)
        print(dict(list(self.items.items())[:num_docs_with_expiry]))
        self.bucket_ops.upsert_items(dict(list(self.items.items())[:num_docs_with_expiry]), expiry_duration)
        self.bucket_ops.upsert_items(dict(list(self.items.items())[num_docs_with_expiry:]))
        self.bucket_ops.close_connection()


class IntiateDataGenerator:

    def __init__(self, num_items, batch_meta):
        self.batch_meta = batch_meta
        self.batch_size = self.batch_meta["schema"]["num_docs"]
        self.num_items = num_items
        self.log = util.initialize_logger("data-generator")

    def initiate(self, start_doc):
        start_document = start_doc
        batches = []
        for i in range(start_document, self.num_items, self.batch_size):
            if i + self.batch_size > start_document + self.num_items:
                end_doc = start_document + self.num_items
                batch_meta_tmp = copy.deepcopy(self.batch_meta)
                batch_meta_tmp["schema"]["num_docs"] = end_doc - i
                batches.append(Batch(i, end_doc, batch_meta_tmp))
            else:
                end_doc = i + self.batch_size
                batches.append(Batch(i, end_doc, self.batch_meta))

        self.log.info("Number of batches : {0}".format(len(batches)))

        with concurrent.futures.ProcessPoolExecutor(max_workers=constants.KV_OPS_PROCESSES) as executor:
            for ins in Batch.get_instances():
                executor.submit(ins.batch_ops)

            executor.shutdown(wait=True)


def insert_batch_temp():
    bucket_ops = BucketOps()
    bucket_ops.create_connection("travel-sample")
    for i in range(62750, 70751):
        item = {"test_" + str(i): {
            "callsign": "Rainbow",
            "country": "United States",
            "iata": "RN",
            "icao": "RAB",
            "id": i,
            "name": "Rainbow Air (RAI)",
            "type": "airline",
            "long_num": random.choice([9223372036854775908])
        }}
        bucket_ops.upsert_items(item)
        print("====================")
        print(str(item))
        print(str(datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')))
        print("====================")
        time.sleep(1)



if __name__ == '__main__':
    #schema = SchemaGenerator().get_schema()
    #batch_meta = {"predefined_values" : 0.2, "random_key": False, "schema": schema, "UPSERT": {"DOCS": 0.2, "RANDOM": False}, "DELETE":
    #    {"DOCS": 0.3, "RANDOM": False}, "EXPIRY": {"DOCS": 0.3, "TIME": 100}}
    #IntiateDataGenerator(1000, batch_meta).initiate()
    insert_batch_temp()
