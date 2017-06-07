__author__ = 'Bohdan Mushkevych'

import unittest

import os

from schema.io import load
from schema.sdpl_schema import Schema, Field, FieldType

TESTS_ROOT = os.path.abspath(os.path.dirname(__file__))


class ProtobufSchemaTest(unittest.TestCase):
    def _load_proto_schema(self, file_name):
        full_path = os.path.join(TESTS_ROOT, file_name)
        yaml_obj = load(full_path)
        return yaml_obj

    def test_valid_schema(self):
        # string query = 1;
        # int32 page_number = 2;
        # int32 result_per_page = 3;
        # string corpus = 4;
        schema = Schema()
        schema.fields.append(Field('query', FieldType.CHARARRAY, is_nullable=False))
        schema.fields.append(Field('page_number', FieldType.INTEGER, is_nullable=False))
        schema.fields.append(Field('result_per_page', FieldType.INTEGER, is_nullable=False))
        schema.fields.append(Field('corpus', FieldType.CHARARRAY, is_nullable=False))

        file_name = 'schema_protobuf.proto|SearchRequest'
        yaml_obj = self._load_proto_schema(file_name)
        self.assertIsInstance(yaml_obj, Schema, 'SDPL PROTO Loader was not successful for {0}'.format(file_name))
        self.assertEqual(schema, yaml_obj)


if __name__ == '__main__':
    unittest.main()
