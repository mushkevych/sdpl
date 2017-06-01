__author__ = 'Bohdan Mushkevych'

import traceback
import unittest

import os

from schema.io import load
from schema.sdpl_schema import Schema, Field, FieldType

TESTS_ROOT = os.path.abspath(os.path.dirname(__file__))


class AvroSchemaTest(unittest.TestCase):
    def _load_avro_schema(self, file_name):
        full_path = os.path.join(TESTS_ROOT, file_name)
        yaml_obj = load(full_path)
        return yaml_obj

    def test_invalid_schema(self):
        file_name = 'error_schema_avro.avsc'
        try:
            self._load_avro_schema(file_name)
            self.assertTrue(False, 'SDPL AVRO Loader exception was expected but not raised for {0}'.format(file_name))
        except Exception:
            traceback.print_exc()
            self.assertTrue(True, 'SDPL AVRO Loader exception was expected and raised for {0}'.format(file_name))

    def test_valid_schema(self):
        # {"name": "name", "type": "string"},
        # {"name": "alpha",  "type": ["int", "null"]},
        # {"name": "beta", "type": ["string", "null"]}
        schema = Schema()
        schema.fields.append(Field('name', FieldType.CHARARRAY, is_nullable=False))
        schema.fields.append(Field('alpha', FieldType.INTEGER, is_nullable=True))
        schema.fields.append(Field('beta', FieldType.CHARARRAY, is_nullable=True))

        file_name = 'schema_avro.avsc'
        yaml_obj = self._load_avro_schema(file_name)
        self.assertIsInstance(yaml_obj, Schema, 'SDPL AVRO Loader was not successful for {0}'.format(file_name))
        self.assertEqual(schema, yaml_obj)


if __name__ == '__main__':
    unittest.main()
