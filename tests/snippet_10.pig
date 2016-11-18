-- File generated by SDPL compiler from tests/snippet_10.sdpl
-- Do not edit the file manually
-- # snippet: testing COMPUTE construct with no *plain* (i.e. uncomputed) fields
-- # load A
A = LOAD 'host.the_company.xyz:6789/mydb/table_name' USING PigStorage(',') AS (
    a:CHARARRAY,
    aa:CHARARRAY,
    aaa:CHARARRAY,
    column:BOOLEAN,
    another_column:BOOLEAN,
    yet_another_column:BOOLEAN
);
-- # load B
B = LOAD 'host.the_company.xyz:6789/mydb/file_blob' USING PigStorage(',') AS (
    b:INTEGER,
    bb:INTEGER,
    bbb:INTEGER,
    column:BOOLEAN,
    another_column:BOOLEAN,
    yet_another_column:BOOLEAN
);
-- # join and compute
JOIN_A_B = JOIN A BY (A.a,A.aa), B BY (B.b,B.bb) ;
C = FOREACH JOIN_A_B GENERATE
    my_function(A.aaa, B.bb, 'aaa', 123) AS func_field,
    SUM(A.aa) AS sum_field,
    A.aa + B.bb AS add_field
;
-- # user formatted comments ahead of STORE C
STORE INTO 'host.the_company.xyz:6789/mydb/table_name' USING PigStorage(',') ;
-- SDPL output: EOF
