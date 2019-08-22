import random
import string
import logging
from datetime import datetime


class DataGenerator:
    # num_docs
    # doc_key_length
    # total_doc_size
    # num_fields
    # For each field :
    ## field_key_length
    ## field_key_value_length
    ## field_key_value_data_type

    def __init__(self, schema):
        self.log = self.initialize_logger("data-generator")

        #self.generate_doc_map()
        self.log.info("Schema : ")
        self.log.info(schema)
        self.log.info("Schema Fields: ")
        self.log.info(schema["fields"])
        self.log.info("Schema Doc Key Length: ")
        self.log.info(schema['doc_key_length'])
        self.generate_docs(schema["doc_key_length"], schema["fields"])

    def initialize_logger(self, logger_name):
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        timestamp = str(datetime.now().strftime('%Y%m%dT_%H%M%S'))
        fh = logging.FileHandler("./{0}-{1}.log".format(logger_name, timestamp))
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        return logger

    def generate_docs(self, doc_key_length, fields):

        json_doc = {}
        doc = {}
        skip_field = False
        doc_key = ''.join(random.choice(string.printable + '!@#$%^&*()_') for _ in range(doc_key_length))
        json_doc[doc_key] = {}

        for field in fields:
            field_data_type = field["field_data_type"]
            if field_data_type.lower() == "boolean":
                field_value = random.choice([True, False])
            elif field_data_type.lower() == "alphanumeric":
                field_value = ''.join(random.choice(string.letters + string.digits) for _ in range(field["field_value_length"]))
            elif field_data_type.lower() == "integer":
                range_start = 10 ** (field["field_value_length"] - 1)
                range_end = (10 ** field["field_value_length"]) - 1
                field_value = random.randint(range_start, range_end)
            elif field_data_type.lower() == "float":
                precision = random.randint(1,8)
                range_start = 10 ** (field["field_value_length"] - 1)
                range_end = (10 ** field["field_value_length"]) - 1
                field_value = round(random.uniform(range_start, range_end), precision)
            elif field_data_type.lower() == "letters":
                field_value = ''.join(random.choice(string.letters) for _ in range(field["field_value_length"]))
            elif field_data_type.lower() == "string":
                field_value = ''.join(random.choice(string.printable + '!@#$%^&*()_') for _ in
                                      range(field["field_value_length"]))
            elif field_data_type.lower() == "spl_chars":
                field_value = ''.join(random.choice(string.whitespace + string.punctuation + '!@#$%^&*()_') for _ in range(field["field_value_length"]))
            #elif field_data_type.lower() == "date":
            elif field_data_type.lower() == "null":
                field_value = None
            elif field_data_type.lower() == "missing":
                field_value = ''.join(random.choice(string.letters) for _ in range(field["field_value_length"]))
                skip_field = random.choice([True, False])
            else:
                self.log.info("unknown data type")

            if not skip_field:
                doc[field["field_name"]] = field_value

        json_doc[doc_key] = doc

        #self.log.info("JSON Doc:")
        #self.log.info(json_doc)

        return json_doc

class SchemaGenerator:

    # {
    #   num_docs:
    #   doc_key_length:
    #   total_doc_size:
    #   fields: {[
    #       field_name:
    #       field_data_type:
    #       field_value_length: ]
    #    }
    # }

    schema_map = None

    def __init__(self, num_docs):
        self.log = self.initialize_logger("schema-generator")
        self.generate_schema(num_docs)

    def initialize_logger(self, logger_name):
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        timestamp = str(datetime.now().strftime('%Y%m%dT_%H%M%S'))
        fh = logging.FileHandler("./{0}-{1}.log".format(logger_name, timestamp))
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        return logger

    def get_schema(self):
        return self.schema_map

    def generate_schema(self, num_docs):
        # Assume MIN_NUM_FIELDS = 3, MAX_NUM_FIELDS=10
        MIN_NUM_FIELDS = 3
        MAX_NUM_FIELDS = 10
        num_fields = random.randint(MIN_NUM_FIELDS, MAX_NUM_FIELDS)

        print num_fields

        field_info_list = []
        max_doc_size = 0
        doc_key_length = random.randint(0, 255)
        for i in xrange(num_fields):
            # Generate Field Key Length between 3 and 100
            field_key_length = random.randint(3, 100)

            field_info = {}
            field_info["field_name"] = ''.join(random.choice(string.lowercase) for x in range(field_key_length))
            field_info["field_data_type"] = random.choice(["string","float","alphanumeric","boolean","integer","letters","spl_chars", "null", "missing"])
            field_info["field_value_length"] = random.randint(3, 20971520)
            field_info_list.append(field_info)

            max_doc_size += field_key_length + field_info["field_value_length"]

        self.schema_map = {}
        self.schema_map["fields"] = field_info_list
        self.schema_map["max_doc_size"] = max_doc_size
        self.schema_map["doc_key_length"] = int(doc_key_length)
        self.schema_map["num_docs"] = num_docs

        print self.schema_map

if __name__ == '__main__':
    schema = SchemaGenerator(10).get_schema()
    DataGenerator(schema)




















