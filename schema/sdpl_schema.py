__author__ = 'Bohdan Mushkevych'

from yaml import YAMLObject
from enum import Enum, unique

VARCHAR_DEFAULT_LENGTH = 256
MIN_VERSION_NUMBER = 1


@unique
class DataType(Enum):
    INTEGER = 1
    LONG = 2
    FLOAT = 3
    CHARARRAY = 4  # STRING
    BYTEARRAY = 5  # BLOB
    BOOLEAN = 6
    DATETIME = 7


class Field(YAMLObject):
    """ module represents a single versioned field in the schema.
        for instance: `name chararray max_length 64 NOT NULL`
        if the field version is higher than the data repository (database) version,
        the field should be excluded from processing
    """
    yaml_tag = '!Field'

    def __init__(self, name: str, data_type: DataType, is_nullable: bool = None, is_unique: bool = None,
                 is_primary_key: bool = None, max_length: int = None, default=None, version=MIN_VERSION_NUMBER):
        assert version >= MIN_VERSION_NUMBER, 'Version number for field {0} must be a positive integer'.format(name)

        self.name = name
        self.data_type = data_type.name
        self.is_nullable = is_nullable
        self.is_unique = is_unique
        self.is_primary_key = is_primary_key
        self.max_length = max_length
        self.default = default
        self.version = version

    def __eq__(self, other):
        """ compares two fields.
            NOTICE: `version` field is not considered """
        return self.name == other.name \
               and self.data_type == other.data_type \
               and self.is_nullable == other.is_nullable \
               and self.is_unique == other.is_unique \
               and self.is_primary_key == other.is_primary_key \
               and self.max_length == other.max_length \
               and self.default == other.default


class Schema(YAMLObject):
    """ module represents collection of schema fields """
    yaml_tag = '!Schema'

    def __init__(self):
        self.fields = list()

    def __eq__(self, other):
        for f in self.fields:
            if f not in other.fields:
                return False
        return True

    def __getitem__(self, item):
        for f in self.fields:
            if f.name == item:
                return f
        raise KeyError('{0} does not match any field in the schema'.format(item))

    @property
    def version(self):
        """ returns maximum field version value """
        return max([f.version for f in self.fields])


class DataRepository(YAMLObject):
    """ module specifies access to the data repository
        this can be: local fs, dfs, s3, rdbms database, etc
    """
    yaml_tag = '!DataRepository'

    def __init__(self, name, host, port, db, user, password, **kwargs):
        self.name = name
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.password = password
        self.kwargs = kwargs

    def __eq__(self, other):
        return self.name == other.name \
               and self.host == other.host \
               and self.port == other.port \
               and self.db == other.db \
               and self.user == other.user \
               and self.password == other.password \
               and self.kwargs == other.kwargs
