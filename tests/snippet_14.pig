-- File generated by SDPL compiler from tests/snippet_14.sdpl
-- Do not edit the file manually
-- # snippet: testing GROUP BY and ORDER BY construct
-- # load A
A = LOAD 's3://my_bucket.s3.amazonaws.com:443/path/within/bucket/table_name' USING PigStorage(',') AS (
    a:CHARARRAY,
    aa:CHARARRAY,
    aaa:CHARARRAY,
    column:BOOLEAN,
    another_column:BOOLEAN,
    yet_another_column:BOOLEAN
);
-- # load B
B = LOAD 's3://my_bucket.s3.amazonaws.com:443/path/within/bucket/file_blob' USING PigStorage(',') AS (
    b:INTEGER,
    bb:INTEGER,
    bbb:INTEGER,
    column:BOOLEAN,
    another_column:BOOLEAN,
    yet_another_column:BOOLEAN
);
-- # ORDER BY clause
C = ORDER A BY A.a, A.aa, A.aaa ;
-- # ORDER BY clause
D = GROUP B BY B.b, B.bb, B.bbb ;
-- # user formatted comments ahead of STORE C
STORE A INTO 's3://my_bucket.s3.amazonaws.com:443/path/within/bucket/table_name' USING PigStorage(',') ;
-- # user formatted comments ahead of STORE D
STORE B INTO 's3://my_bucket.s3.amazonaws.com:443/path/within/bucket/table_name' USING PigStorage(',') ;
-- SDPL output: EOF
