__author__ = 'Bohdan Mushkevych'

from schema.sdpl_schema import Schema, Field, MIN_VERSION_NUMBER, DataType, FieldType
from parser.data_store import DataStore


PGSQL_MAPPING = {
    FieldType.INTEGER.name: 'IntegerType',
    FieldType.LONG.name: 'LongType',
    FieldType.FLOAT.name: 'FloatType',
    FieldType.CHARARRAY.name: 'StringType',
    FieldType.BYTEARRAY.name: 'BinaryType',
    FieldType.BOOLEAN.name: 'BooleanType',
    FieldType.DATETIME.name: 'TimestampType',
}


def parse_field(field: Field):
    out = 'StructField("{0}", {1}, {2})'.format(field.name, field.field_type, field.is_nullable)
    return out


def parse_schema(schema: Schema, max_version=MIN_VERSION_NUMBER):
    filtered_fields = [f for f in schema.fields if f.version <= max_version]
    out = ',\n    '.join([parse_field(field) for field in filtered_fields])
    out = 'StructType([ {0} ])\n'.format(out)
    return out


def _jdbc_datasink(data_sink: DataStore):
    jdbc_uri = 'jdbc:postgresql://{0}:{1}/{2}'.format(data_sink.data_repository.host,
                                                      data_sink.data_repository.port,
                                                      data_sink.data_repository.db)

    jdbc_properties = "{ 'user': '{0}', 'password': '{1}' }".format(data_sink.data_repository.user,
                                                                    data_sink.data_repository.password)
    store_string = "sqlContext.write.jdbc({0}, {1}, properties={2})". \
        format(jdbc_uri, data_sink.table_name, jdbc_properties)

    return store_string


def _file_datasink(data_sink: DataStore):
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

    if data_sink.data_repository.data_type == DataType.CSV.name:
        store_string = "sqlContext.write.csv({0}, compression='{1}', sep=',')".\
            format(fqfp, data_sink.data_repository.compression)
    elif data_sink.data_repository.data_type == DataType.TSV.name:
        store_string = "sqlContext.write.csv({0}, compression='{1}', sep='\t')".\
            format(fqfp, data_sink.data_repository.compression)
    elif data_sink.data_repository.data_type == DataType.BIN.name:
        store_string = "TBD"
    elif data_sink.data_repository.data_type == DataType.JSON.name:
        store_string = "sqlContext.write.json({0}, compression='{1}')".\
            format(fqfp, data_sink.data_repository.compression)
    elif data_sink.data_repository.data_type == DataType.ORC.name:
        store_string = "sqlContext.write.orc({0}, compression='{1}')".\
            format(fqfp, data_sink.data_repository.compression)
    else:
        store_string = "sqlContext.write.text({0}, compression='{1}')".\
            format(fqfp, data_sink.data_repository.compression)

    return store_string


def parse_datasink(data_sink: DataStore):
    if data_sink.data_repository.is_file_type:
        _file_datasink(data_sink)
    else:
        _jdbc_datasink(data_sink)


def _jdbc_datasource(data_source: DataStore):
    jdbc_uri = 'jdbc:postgresql://{0}:{1}/{2}'.format(data_source.data_repository.host,
                                                      data_source.data_repository.port,
                                                      data_source.data_repository.db)

    jdbc_properties = "{ 'user': '{0}', 'password': '{1}' }".format(data_source.data_repository.user,
                                                                    data_source.data_repository.password)
    load_string = "sqlContext.read.jdbc({0}, {1}, properties={2})". \
        format(jdbc_uri, data_source.table_name, jdbc_properties)

    return load_string


def _file_datasource(data_source: DataStore):
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

    if data_source.data_repository.data_type == DataType.CSV.name:
        load_string = "sqlContext.read.csv({0}, schema={1}, sep=',')".\
            format(fqfp, parse_schema(data_source.relation.schema))
    elif data_source.data_repository.data_type == DataType.TSV.name:
        load_string = "sqlContext.read.csv({0}, schema={1}, sep='\t')".\
            format(fqfp, parse_schema(data_source.relation.schema))
    elif data_source.data_repository.data_type == DataType.BIN.name:
        load_string = "TBD"
    elif data_source.data_repository.data_type == DataType.JSON.name:
        load_string = "sqlContext.read.json({0}, schema={1})".format(fqfp, parse_schema(data_source.relation.schema))
    elif data_source.data_repository.data_type == DataType.ORC.name:
        load_string = "sqlContext.read.orc({0})".format(fqfp)
    else:
        load_string = "sqlContext.read.text({0})".format(fqfp)

    return load_string


def parse_datasource(data_source: DataStore):
    if data_source.data_repository.is_file_type:
        _file_datasource(data_source)
    else:
        _jdbc_datasource(data_source)
