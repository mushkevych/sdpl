__author__ = 'Bohdan Mushkevych'

from schema.sdpl_schema import Schema, Field, MIN_VERSION_NUMBER, DataType, Compression
from parser.data_store import DataStore


def parse_field(field: Field):
    out = '{0}:{1}'.format(field.name, field.field_type)
    return out


def parse_schema(schema: Schema, max_version=MIN_VERSION_NUMBER):
    filtered_fields = [f for f in schema.fields if f.version <= max_version]
    out = ',\n    '.join([parse_field(field) for field in filtered_fields])
    out = '\n    ' + out + '\n'
    return out


def _jdbc_datasink(data_sink: DataStore):
    field_names = [f.name for f in data_sink.relation.schema.fields]
    values = ['?' for _ in range(len(field_names))]

    out = "REGISTER /var/lib/sdpl/postgresql-42.0.0.jar;\n"
    out += "REGISTER /var/lib/sdpl/piggybank-0.16.0.jar;\n"
    out += "STORE {0} INTO 'hdfs:///unused-ignore' ".format(data_sink.relation.name)
    out += "USING org.apache.pig.piggybank.storage.DBStorage(\n"
    out += "    'org.postgresql.Driver',\n"
    out += "    'jdbc:postgresql://{0}:{1}/{2}',\n".format(data_sink.data_repository.host,
                                                           data_sink.data_repository.port,
                                                           data_sink.data_repository.db)
    out += "    '{0}', '{1}',\n".format(data_sink.data_repository.user, data_sink.data_repository.password)
    out += "    'INSERT INTO {0} ({1}) VALUES ({2})'\n".format(data_sink.table_name,
                                                               ','.join(field_names), ','.join(values))
    out += ');'
    return out


def _file_datasink(data_sink: DataStore):
    if data_sink.data_repository.data_type == DataType.CSV.name:
        store_function = "PigStorage(',')"
    elif data_sink.data_repository.data_type == DataType.TSV.name:
        store_function = "PigStorage()"
    elif data_sink.data_repository.data_type == DataType.BIN.name:
        store_function = "BinStorage()"
    elif data_sink.data_repository.data_type == DataType.JSON.name:
        store_function = "JsonStorage()"
    elif data_sink.data_repository.data_type == DataType.ORC.name:
        is_snappy = data_sink.data_repository.compression == Compression.SNAPPY.name
        store_function = "OrcStorage('-c SNAPPY')" if is_snappy else "OrcStorage()"
    else:
        store_function = "PigStorage()"

    if not data_sink.data_repository.host:
        # local file system
        fqfp = '/{0}/{1}'.format(data_sink.data_repository.db.strip('/'),
                                 data_sink.table_name)
    else:
        # distributed file system
        fqfp = '{0}:{1}/{2}/{3}'.format(data_sink.data_repository.host.strip('/'),
                                        data_sink.data_repository.port,
                                        data_sink.data_repository.db.strip('/'),
                                        data_sink.table_name)

    store_string = "STORE {0} INTO '{1}' USING {2} ;".format(data_sink.relation.name, fqfp, store_function)
    return store_string


def parse_datasink(data_sink: DataStore):
    if data_sink.data_repository.is_file_type:
        _file_datasink(data_sink)
    else:
        _jdbc_datasink(data_sink)


def _jdbc_datasource(data_source: DataStore):
    raise NotImplementedError('pig_schema._jdbc_datasource is not supported')


def _file_datasource(data_source: DataStore):
    if data_source.data_repository.data_type == DataType.CSV.name:
        load_function = "PigStorage(',')"
    elif data_source.data_repository.data_type == DataType.TSV.name:
        load_function = "PigStorage()"
    elif data_source.data_repository.data_type == DataType.BIN.name:
        load_function = "BinStorage()"
    elif data_source.data_repository.data_type == DataType.JSON.name:
        load_function = "JsonLoader()"
    elif data_source.data_repository.data_type == DataType.ORC.name:
        is_snappy = data_source.data_repository.compression == Compression.SNAPPY.name
        load_function = "OrcStorage('-c SNAPPY')" if is_snappy else "OrcStorage()"
    else:
        load_function = "PigStorage()"

    if not data_source.data_repository.host:
        # local file system
        fqfp = '/{0}/{1}'.format(data_source.data_repository.db.strip('/'),
                                 data_source.table_name)
    else:
        # distributed file system
        fqfp = '{0}:{1}/{2}/{3}'.format(data_source.data_repository.host.strip('/'),
                                        data_source.data_repository.port,
                                        data_source.data_repository.db.strip('/'),
                                        data_source.table_name)

    load_string = "LOAD '{0}' USING {1} AS ({2})".format(fqfp, load_function, parse_schema(data_source.relation.schema))
    return load_string


def parse_datasource(data_source: DataStore):
    if data_source.data_repository.is_file_type:
        _file_datasource(data_source)
    else:
        _jdbc_datasource(data_source)
