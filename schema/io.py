__author__ = 'Bohdan Mushkevych'

import os
import avro
from yaml import load as yaml_load, dump, YAMLObject

from schema.avro_schema import AvroSchema

EXTENTION_AVRO = 'avsc'
EXTENTION_SDPL = 'yaml'


def load(input_path:str):
    extension = os.path.basename(input_path).split('.')[-1]
    with open(input_path, mode='r', encoding='utf-8') as input_stream:
        if extension == EXTENTION_SDPL:
            return yaml_load(input_stream)
        elif extension == EXTENTION_AVRO:
            s = avro.schema.Parse(input_stream.read())
            return AvroSchema(s).to_sdpl_schema()
        else:
            raise ValueError('unknown schema file extension {0}'.format(extension))


def store(yaml_object:YAMLObject, output_path:str):
    output_path = output_path.strip('\'')
    with open(output_path, 'w', encoding='utf-8') as output_stream:
        dump(yaml_object, output_stream)
