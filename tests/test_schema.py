__author__ = 'Bohdan Mushkevych'

import os
import traceback
import unittest

from schema.io import load, store
from schema.sdpl_schema import DataRepository, Schema, Field, DataType
from parser.projection import RelationProjection
from parser.relation import Relation

TESTS_ROOT = os.path.abspath(os.path.dirname(__file__))


def create_schemas():
    schema_a = Schema()
    schema_a.fields.append(Field('a', DataType.CHARARRAY))
    schema_a.fields.append(Field('aa', DataType.CHARARRAY))
    schema_a.fields.append(Field('aaa', DataType.CHARARRAY))
    schema_a.fields.append(Field('column', DataType.BOOLEAN))
    schema_a.fields.append(Field('another_column', DataType.BOOLEAN))
    schema_a.fields.append(Field('yet_another_column', DataType.BOOLEAN))
    # store schema
    store(schema_a, os.path.join(TESTS_ROOT, 'schema_a.yaml'))

    schema_b = Schema()
    schema_b.fields.append(Field('b', DataType.INTEGER))
    schema_b.fields.append(Field('bb', DataType.INTEGER))
    schema_b.fields.append(Field('bbb', DataType.INTEGER))
    schema_b.fields.append(Field('column', DataType.BOOLEAN))
    schema_b.fields.append(Field('another_column', DataType.BOOLEAN))
    schema_b.fields.append(Field('yet_another_column', DataType.BOOLEAN))
    # store schema
    store(schema_b, os.path.join(TESTS_ROOT, 'schema_b.yaml'))

    schema_c = Schema()
    schema_c.fields.append(Field('c', DataType.LONG))
    schema_c.fields.append(Field('cc', DataType.LONG))
    schema_c.fields.append(Field('ccc', DataType.LONG))
    schema_c.fields.append(Field('column', DataType.BOOLEAN))
    schema_c.fields.append(Field('another_column', DataType.BOOLEAN))
    schema_c.fields.append(Field('yet_another_column', DataType.BOOLEAN))
    # store schema
    store(schema_c, os.path.join(TESTS_ROOT, 'schema_c.yaml'))

    schema_v = Schema()
    schema_v.fields.append(Field('v', DataType.LONG))
    schema_v.fields.append(Field('vv', DataType.LONG))
    schema_v.fields.append(Field('vvv', DataType.LONG))
    schema_v.fields.append(Field('column', DataType.BOOLEAN))
    schema_v.fields.append(Field('min_version', DataType.BOOLEAN))
    schema_v.fields.append(Field('middle_version', DataType.BOOLEAN, version=2))
    schema_v.fields.append(Field('high_version', DataType.BOOLEAN, version=4))
    # store schema
    store(schema_v, os.path.join(TESTS_ROOT, 'schema_v.yaml'))

    schemas = {
        os.path.join(TESTS_ROOT, 'schema_a.yaml'): schema_a,
        os.path.join(TESTS_ROOT, 'schema_b.yaml'): schema_b,
        os.path.join(TESTS_ROOT, 'schema_c.yaml'): schema_c,
        os.path.join(TESTS_ROOT, 'schema_v.yaml'): schema_v
    }
    return schemas


def create_repo():
    # declare database/repository
    repo_a = DataRepository('repo_a', 'host.the_company.xyz', '6789', 'mydb', 'the_user', 'the_password')
    store(repo_a, os.path.join(TESTS_ROOT, 'repo_a.yaml'))

    schemas = {
        os.path.join(TESTS_ROOT, 'repo_a.yaml'): repo_a
    }
    return schemas


def create_relations(schema_paths):
    def _extract_schema_name(full_path):
        basename = os.path.basename(full_path)
        schema_name = basename.split('.')[0]
        return schema_name[len('schema_'):].upper()

    relations = dict()
    for full_path, schema in schema_paths.items():
        schema_name = _extract_schema_name(full_path)
        relation = Relation(schema_name, None)
        relation._schema = schema
        relations[schema_name] = relation
    return relations


class SchemaTest(unittest.TestCase):
    def setUp(self):
        # format: {path : Schema}
        self.schema_paths = create_schemas()

        # format: {path : Repo}
        self.repo_paths = create_repo()

        # format: {name : Relation}
        self.relations = create_relations(self.schema_paths)

    def tearDown(self):
        pass

    def _test_collection(self, collection):
        for full_path, schema in collection.items():
            try:
                yaml_obj = load(full_path)
                self.assertEqual(schema, yaml_obj, 'SDPL YAML Loader was successfully for {0}'.format(full_path))
            except Exception:
                traceback.print_exc()
                self.assertTrue(False, 'SDPL YAML Loader exception was raised for {0}'.format(full_path))

    def test_loader(self):
        self._test_collection(self.schema_paths)
        self._test_collection(self.repo_paths)

    def test_projection(self):
        schema = Schema()
        schema.fields.append(Field('a', DataType.CHARARRAY))
        schema.fields.append(Field('aa', DataType.CHARARRAY))
        schema.fields.append(Field('aaa', DataType.CHARARRAY))
        schema.fields.append(Field('column', DataType.BOOLEAN))
        schema.fields.append(Field('another_column', DataType.BOOLEAN))
        schema.fields.append(Field('yet_another_column', DataType.BOOLEAN))
        schema.fields.append(Field('bbb', DataType.INTEGER))

        projection = RelationProjection(self.relations)
        projection.add_all('A')
        projection.add('B', 'bbb')
        self.assertEqual(schema, projection.finalize_relation('UT').schema)

        schema = Schema()
        schema.fields.append(Field('a', DataType.CHARARRAY))
        schema.fields.append(Field('aa', DataType.CHARARRAY))
        schema.fields.append(Field('aaa', DataType.CHARARRAY))

        projection = RelationProjection(self.relations)
        projection.add_all('A')
        projection.remove_all('B')
        self.assertEqual(schema, projection.finalize_relation('UT').schema)

    def test_projection_duplicates(self):
        schema = Schema()
        schema.fields.append(Field('a', DataType.CHARARRAY))
        schema.fields.append(Field('aa', DataType.CHARARRAY))
        schema.fields.append(Field('aaa', DataType.CHARARRAY))
        schema.fields.append(Field('column', DataType.BOOLEAN))
        schema.fields.append(Field('another_column', DataType.BOOLEAN))
        schema.fields.append(Field('yet_another_column', DataType.BOOLEAN))
        schema.fields.append(Field('b', DataType.INTEGER))
        schema.fields.append(Field('bb', DataType.INTEGER))
        schema.fields.append(Field('bbb', DataType.INTEGER))

        projection = RelationProjection(self.relations)
        projection.add_all('A')
        projection.add_all('B')
        projection.remove('B', 'column')
        projection.remove('B', 'column')
        projection.remove('B', 'another_column')
        projection.remove('B', 'yet_another_column')
        self.assertEqual(schema, projection.finalize_relation('UT').schema)

        schema = Schema()
        schema.fields.append(Field('a', DataType.CHARARRAY))
        schema.fields.append(Field('aa', DataType.CHARARRAY))
        schema.fields.append(Field('aaa', DataType.CHARARRAY))
        schema.fields.append(Field('column', DataType.BOOLEAN))
        schema.fields.append(Field('another_column', DataType.BOOLEAN))
        schema.fields.append(Field('yet_another_column', DataType.BOOLEAN))
        schema.fields.append(Field('b', DataType.INTEGER))
        schema.fields.append(Field('bb', DataType.INTEGER))
        schema.fields.append(Field('bbb', DataType.INTEGER))
        schema.fields.append(Field('c', DataType.LONG))
        schema.fields.append(Field('cc', DataType.LONG))
        schema.fields.append(Field('ccc', DataType.LONG))

        projection = RelationProjection(self.relations)
        projection.add_all('A')
        projection.add_all('B')
        projection.add_all('C')
        projection.remove('B', 'column')
        projection.remove('B', 'another_column')
        projection.remove('B', 'yet_another_column')
        projection.remove('C', 'column')
        projection.remove('C', 'another_column')
        projection.remove('C', 'yet_another_column')
        self.assertEqual(schema, projection.finalize_relation('UT').schema)


if __name__ == '__main__':
    unittest.main()
