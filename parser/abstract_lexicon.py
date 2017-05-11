__author__ = 'Bohdan Mushkevych'

from io import TextIOWrapper

from parser.data_store import DataStore
from parser.projection import RelationProjection, FieldProjection
from schema.sdpl_schema import Field, Schema, MIN_VERSION_NUMBER


class AbstractLexicon(object):
    def __init__(self, output_stream: TextIOWrapper) -> None:
        self.output_stream = output_stream

    def _out(self, text):
        self.output_stream.write(text)
        self.output_stream.write('\n')

    def parse_datasource(self, data_source: DataStore):
        pass

    def parse_datasink(self, data_sink: DataStore):
        pass

    def parse_field(self, field: Field):
        pass

    def parse_field_projection(self, field: FieldProjection):
        pass

    def parse_schema(self, schema: Schema, max_version=MIN_VERSION_NUMBER):
        pass

    def emit_udf_registration(self, udf_fqfp: str, udf_alias:str):
        pass

    def emit_releation_decl(self, relation_name: str, data_source: DataStore):
        pass

    def emit_schema_projection(self, left_relation_name: str, right_relation_name: str, output_fields: list):
        pass

    def emit_join(self, relation_name: str, join_elements: dict, projection: RelationProjection) -> None:
        pass
