import random
import string
from CommonUtil import util, constants


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

    def __init__(self):
        self.log = util.initialize_logger("schema-generator")
        self.generate_schema()

    def get_schema(self):
        return self.schema_map

    def generate_schema(self):
        # Assume MIN_NUM_FIELDS = 3, MAX_NUM_FIELDS=10
        self.log.info("Generating schema")
        MIN_NUM_FIELDS = 3
        MAX_NUM_FIELDS = 10
        num_fields = random.randint(MIN_NUM_FIELDS, MAX_NUM_FIELDS)

        self.log.info("Number of fields : {0}".format(num_fields))

        field_info_list = []
        max_doc_size = 0
        doc_key_length = random.randint(0, 255)
        for i in range(num_fields):
            # Generate Field Key Length between 3 and 100
            self.log.info("Field : {0}".format(i))
            field_key_length = random.randint(3, 100)

            field_info = {}
            field_info["field_name"] = ''.join(random.choice(string.ascii_lowercase) for x in range(field_key_length))
            field_info["field_data_type"] = random.choice(
                ["string", "float", "alphanumeric", "boolean", "integer", "letters", "spl_chars", "null", "missing"])
            field_info["field_value_length"] = random.randint(3, 10)
            #field_info["predefined_values"] = self.get_predefined_values()

            self.log.info("Field info : {0}".format(field_info))

            field_info_list.append(field_info)

            max_doc_size += field_key_length + field_info["field_value_length"]

        self.schema_map = {}
        self.schema_map["fields"] = field_info_list
        self.schema_map["max_doc_size"] = max_doc_size
        self.schema_map["doc_key_length"] = int(doc_key_length)
        self.schema_map["num_docs"] = int(constants.MAX_BATCH_SIZE / (max_doc_size * constants.KV_OPS_PROCESSES))

        self.log.info("Schema map : {0}".format(self.schema_map))
        self.log.info("Max doc size : {0}".format(max_doc_size))
