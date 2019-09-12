import random
import string
from CommonUtil import constants, util
from schemagenerator import SchemaGenerator
from couchbase_ops.clustersetup import ClusterSetup


class IndexGenerator():
    # Get the fields from the schema map
    # Fuzz out number of indexes required -- depending upon number of nodes..
    # Dont create more than 7-8 indexes per node. Min index per node = 1
    # Determine number of index nodes in the cluster from clustersetup.py
    # For each index to be created fuzz out like following
    # 1. Primary / Secondary ?
    # 2. If secondary, Single field / composite index ?
    # 2. Which field(s) ?
    # 3. Partitioned ?
    # 4. Adaptive index ?
    # 5. Array index ?
    # 6. meta() fields to be indexed ?
    # 7. Deferred ?

    def __init__(self, schema, num_index_nodes):
        self.log = util.initialize_logger("index-generator")
        self.num_index_nodes = int(num_index_nodes)
        self.schema = schema
        self.generate_index_definitions()

    def generate_index_definitions(self):

        # Fuzz the number of indexes
        num_indexes = random.randint(self.num_index_nodes, self.num_index_nodes * int(constants.MAX_NUM_INDEX_PER_NODE))

        self.log.info("No. of indexes : {0}".format(num_indexes))

        field_names = []
        for field in self.schema["fields"]:
            field_names.append(field["field_name"])

        generated_index_field_combinations_str = []
        index_schema_dict = []

        # Flag for primary index already created
        is_primary_index_created = False

        for i in range(num_indexes):

            # Fuzz index type from primary, secondary index or adaptive index

            is_primary_index = False
            is_adaptive_index = False
            # Index type dictionary with probability weightage
            index_type_dict = {"primary": 4, "secondary": 15, "adaptive": 1}

            index_type = random.choice([k for k in index_type_dict for dummy in range(index_type_dict[k])])
            if index_type == "primary":
                is_primary_index = True
            elif index_type == "adaptive":
                is_adaptive_index = True

            self.log.info("Index Type : {0}".format(index_type))

            # We have a secondary index to be created
            if (not is_primary_index) & (not is_adaptive_index):
                # Select num_fields in the index
                num_fields_in_index = random.randint(1, min(len(self.schema["fields"]), constants.MAX_FIELDS_PER_INDEX))
                # Generate list of fields to be indexed
                this_index_fields = []
                for j in range(num_fields_in_index):
                    index_field = random.choice(field_names)
                    # Check if the field has already been selected for the same index. If so, regenerate the field.
                    if index_field not in this_index_fields:
                        this_index_fields.append(index_field)
                    else:
                        j -= 1

                # Fuzz if the meta fields are to be indexed (20% probability)
                is_meta_fields_indexed = random.choice([False, True, False, False, False])
                if is_meta_fields_indexed:
                    num_meta_fields_indexed = random.randint(1, 3)
                    for j in range(num_meta_fields_indexed):
                        index_field = random.choice(constants.META_FIELDS)
                        # Check if the field has already been selected for the same index. If so, regenerate the field.
                        if index_field not in this_index_fields:
                            this_index_fields.append(index_field)
                        else:
                            j -= 1

                # Save the index fields combination as a str
                this_index_fields_str = ""
                for this_index_field in this_index_fields:
                    if this_index_fields_str == "":
                        this_index_fields_str += this_index_field
                    else:
                        this_index_fields_str = this_index_fields_str + "-" + this_index_field
                # Check if the current index's field combination is not already generated,
                # else regenerate the combination
                if this_index_fields_str not in generated_index_field_combinations_str:
                    generated_index_field_combinations_str.append(this_index_fields_str)

                    # Fuzz if the index is going to be a partitioned index or not.
                    # 33% probability
                    is_partitioned_index = random.choice([True, False, False])

                    # Fuzz if the index build is going to be deferred or immediate
                    # 40% probability
                    is_deferred_index = random.choice([True, False, False, False, True])

                    # Fuzz the count of index replicas
                    idx_num_replica = random.randint(0, self.num_index_nodes-1)

                    # Jump the number of indexes counter as we have created some replica indexes
                    if idx_num_replica > 0:
                        i += idx_num_replica

                    index_name = constants.BUCKET_NAME + "_" + this_index_fields_str
                    index_schema = {"index_name": index_name,
                                    "is_primary": False,
                                    "indexed_field_list": this_index_fields,
                                    "is_partitioned_index": is_partitioned_index,
                                    "is_deferred": is_deferred_index,
                                    "idx_num_replica": idx_num_replica}

                    self.log.info("Index Schema:")
                    self.log.info(index_schema)
                    index_schema_dict.append(index_schema)

                else:
                    i -= 1

            else:
                if is_primary_index:
                    # Primary index
                    # Fuzz if the index is going to be a partitioned index or not.
                    # 33% probability
                    is_partitioned_index = random.choice([True, False, False])

                    # Fuzz if the index build is going to be deferred or immediate
                    # 40% probability
                    is_deferred_index = random.choice([True, False, False, False, True])

                    # Fuzz the count of index replicas
                    idx_num_replica = random.randint(0, self.num_index_nodes - 1)

                    # Jump the number of indexes counter as we have created some replica indexes
                    if idx_num_replica > 0:
                        i += idx_num_replica

                    index_name = constants.BUCKET_NAME + "_primary"
                    index_schema = {"index_name": index_name,
                                    "is_primary": True,
                                    "indexed_field_list": None,
                                    "is_partitioned_index": is_partitioned_index,
                                    "is_deferred": is_deferred_index,
                                    "idx_num_replica": idx_num_replica}
                    if not is_primary_index_created:
                        self.log.info("Index Schema:")
                        self.log.info(index_schema)
                        index_schema_dict.append(index_schema)
                        is_primary_index_created = True
                else:
                    # Adaptive index
                    index_name = constants.BUCKET_NAME + "_adaptive"
                    index_schema = {"index_name": index_name,
                                    "is_primary": False,
                                    "is_adaptive": True,
                                    "indexed_field_list": None,
                                    "is_partitioned_index": False,
                                    "is_deferred": False,
                                    "idx_num_replica": False}

                    # If creating adaptive index, there is no need of creating any other index for this test. Drop everything.
                    index_schema_dict = []
                    index_schema_dict.append(index_schema)
                    self.log.info("Index Schema:")
                    self.log.info(index_schema)
                    self.log.info("Since we are creating an adaptive index, no other index is required in this test.")
                    break

        self.log.info("Index List :")
        self.log.info(index_schema_dict)

    def generate_docs(self, doc_key_length, fields):

        json_doc = {}
        doc = {}
        skip_field = False
        doc_key = ''.join(random.choice(string.printable + '!@#$%^&*()_') for _ in range(doc_key_length))
        json_doc[doc_key] = {}
        field_value = None

        for field in fields:
            field_data_type = field["field_data_type"]
            if field_data_type.lower() == "boolean":
                field_value = random.choice([True, False])
            elif field_data_type.lower() == "alphanumeric":
                field_value = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(field["field_value_length"]))
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
                field_value = ''.join(random.choice(string.ascii_letters) for _ in range(field["field_value_length"]))
            elif field_data_type.lower() == "string":
                field_value = ''.join(random.choice(string.printable + '!@#$%^&*()_') for _ in
                                      range(field["field_value_length"]))
            elif field_data_type.lower() == "spl_chars":
                field_value = ''.join(random.choice(string.whitespace + string.punctuation + '!@#$%^&*()_') for _ in range(field["field_value_length"]))
            #elif field_data_type.lower() == "date":
            elif field_data_type.lower() == "null":
                field_value = None
            elif field_data_type.lower() == "missing":
                field_value = ''.join(random.choice(string.ascii_letters) for _ in range(field["field_value_length"]))
                skip_field = random.choice([True, False])
            else:
                self.log.info("unknown data type")

            if not skip_field:
                doc[field["field_name"]] = field_value

        json_doc[doc_key] = doc

        #self.log.info("JSON Doc:")
        #self.log.info(json_doc)

        return json_doc

if __name__ == '__main__':
    schema = SchemaGenerator().get_schema()
    clustersetup = ClusterSetup()
    num_index_nodes = clustersetup.get_num_index_nodes()
    IndexGenerator(schema, num_index_nodes)




















