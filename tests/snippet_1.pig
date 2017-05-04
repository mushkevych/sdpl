-- File generated by SDPL compiler from tests/snippet_1.sdpl
-- Do not edit the file manually
-- # snippet: load schema, load data, perform joins and schema projections, store result
REGISTER 'file://path_to_library' AS first_library;
A = LOAD 's3://my_bucket.s3.amazonaws.com:443/path/within/bucket/table_name' USING PigStorage(',') AS (
    a:CHARARRAY,
    aa:CHARARRAY,
    aaa:CHARARRAY,
    column:BOOLEAN,
    another_column:BOOLEAN,
    yet_another_column:BOOLEAN
);
B = LOAD 's3://my_bucket.s3.amazonaws.com:443/path/within/bucket/file_blob' USING PigStorage(',') AS (
    b:INTEGER,
    bb:INTEGER,
    bbb:INTEGER,
    column:BOOLEAN,
    another_column:BOOLEAN,
    yet_another_column:BOOLEAN
);
C = LOAD 's3://my_bucket.s3.amazonaws.com:443/path/within/bucket/file_name' USING PigStorage(',') AS (
    c:LONG,
    cc:LONG,
    ccc:LONG,
    column:BOOLEAN,
    another_column:BOOLEAN,
    yet_another_column:BOOLEAN
);
JOIN_A_B = JOIN A BY (A.a), B BY (B.b) ;
D = FOREACH JOIN_A_B GENERATE
    A.a AS a,
    A.aa AS aa,
    A.aaa AS aaa,
    A.column AS column,
    A.another_column AS another_column,
    A.yet_another_column AS yet_another_column,
    B.column AS new_column
;
JOIN_C_D = JOIN C BY (C.c), D BY (D.new_column) ;
E = FOREACH JOIN_C_D GENERATE
    C.c AS c,
    C.cc AS cc,
    C.ccc AS ccc,
    C.column AS column,
    C.another_column AS another_column,
    C.yet_another_column AS yet_another_column,
    D.new_column AS new_column,
    D.column AS renamed_column
;
STORE D INTO 's3://my_bucket.s3.amazonaws.com:443/path/within/bucket/table_name' USING PigStorage(',') ;
-- SDPL output: EOF
