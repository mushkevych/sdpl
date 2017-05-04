-- File generated by SDPL compiler from tests/snippet_13.sdpl
-- Do not edit the file manually
-- # snippet: load data from all supported types of sources, store data in all supported types of sink
A = LOAD 's3://my_bucket.s3.amazonaws.com:443/path/within/bucket/table_name' USING PigStorage(',') AS (
    a:CHARARRAY,
    aa:CHARARRAY,
    aaa:CHARARRAY,
    column:BOOLEAN,
    another_column:BOOLEAN,
    yet_another_column:BOOLEAN
);
B = LOAD '/data/hive/db_name/file_blob' USING PigStorage() AS (
    b:INTEGER,
    bb:INTEGER,
    bbb:INTEGER,
    column:BOOLEAN,
    another_column:BOOLEAN,
    yet_another_column:BOOLEAN
);
JOIN_A_B = JOIN A BY (A.a,A.aa), B BY (B.b,B.bb) ;
Z = FOREACH JOIN_A_B GENERATE
    A.a AS a,
    A.aa AS aa,
    A.aaa AS aaa,
    A.column AS column,
    A.another_column AS another_column,
    A.yet_another_column AS yet_another_column,
    B.b AS b,
    B.bb AS bb,
    B.bbb AS bbb
;
REGISTER /var/lib/sdpl/postgresql-42.0.0.jar;
REGISTER /var/lib/sdpl/piggybank-0.16.0.jar;
STORE Z INTO 'hdfs:///unused-ignore' USING org.apache.pig.piggybank.storage.DBStorage(
    'org.postgresql.Driver',
    'jdbc:postgresql://host.the_company.xyz:6789/mydb',
    'the_user', 'the_password',
    'INSERT INTO db_schema.db_table_name (a,aa,aaa,column,another_column,yet_another_column,b,bb,bbb) VALUES (?,?,?,?,?,?,?,?,?)'
);
STORE Z INTO 's3://my_bucket.s3.amazonaws.com:443/path/within/bucket/my_file.gz' USING PigStorage(',') ;
STORE Z INTO '/data/hive/db_name/my_file' USING PigStorage() ;
-- SDPL output: EOF