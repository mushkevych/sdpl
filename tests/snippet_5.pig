-- File generated by SDPL compiler from tests/snippet_5.sdpl
-- Do not edit the file manually
-- # snippet: multi-relational joins
A = LOAD 'host.the_company.xyz:6789/mydb/table_name' USING PigStorage(',') AS (
    a:CHARARRAY,
    aa:CHARARRAY,
    aaa:CHARARRAY,
    column:BOOLEAN,
    another_column:BOOLEAN,
    yet_another_column:BOOLEAN
);
B = LOAD 'host.the_company.xyz:6789/mydb/file_blob' USING PigStorage(',') AS (
    b:INTEGER,
    bb:INTEGER,
    bbb:INTEGER,
    column:BOOLEAN,
    another_column:BOOLEAN,
    yet_another_column:BOOLEAN
);
C = FILTER A BY A.a == 3 AND (A.aaa >= 0 OR A.aa < -100 OR (A.a != 12.12 AND A.a > 789) ) ;
D = FILTER B BY B.b != 'Bebe' OR B.bb == 'Zeze' ;
E = FILTER B BY B.b != 'table' AND B.bb == 'Bebe' OR B.bbb > -100;
F = FILTER B BY (B.b != 'Bebe');
STORE INTO 'host.the_company.xyz:6789/mydb/table_name_c' USING PigStorage(',') ;
STORE INTO 'host.the_company.xyz:6789/mydb/table_name_d' USING PigStorage(',') ;
-- SDPL output: EOF
