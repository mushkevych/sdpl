__author__ = 'Bohdan Mushkevych'

import os
import avro
import importlib
from yaml import load as yaml_load, dump, YAMLObject

from schema.avro_schema import AvroSchema
from schema.protobuf_schema import ProtobufSchema

EXTENSION_AVRO = 'avsc'
EXTENSION_SDPL = 'yaml'
EXTENSION_PROTOBUF = 'proto'


def load(input_path:str, model_name=None):
    extension = os.path.basename(input_path).split('.')[-1]
    with open(input_path, mode='r', encoding='utf-8') as input_stream:
        if extension == EXTENSION_SDPL:
            return yaml_load(input_stream)
        elif extension == EXTENSION_AVRO:
            s = avro.schema.Parse(input_stream.read())
            return AvroSchema(s).to_sdpl_schema()
        elif extension == EXTENSION_PROTOBUF:
            if not model_name:
                raise ValueError('optional attribute model_name is required to parse .proto schema')

            file_name = os.path.basename(input_path)
            file_name = file_name.replace('.proto', '_pb2')
            i = importlib.import_module(file_name)
            s = getattr(i, model_name)
            return ProtobufSchema(s).to_sdpl_schema()
        else:
            raise ValueError('unknown schema file extension {0}'.format(extension))


def store(yaml_object:YAMLObject, output_path:str):
    output_path = output_path.strip('\'')
    with open(output_path, 'w', encoding='utf-8') as output_stream:
        dump(yaml_object, output_stream)
