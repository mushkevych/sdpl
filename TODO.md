## TODO

### version +1

1. Remove keyword `SCHEMA` from `SCHEMA PROJECTION` 

### version +2

1. Simplify use of the relation name in the field resolution:


    C = FILTER A BY A.a == 3; 
    -> 
    C = FILTER A BY a == 3; 

1. Parse `DataSink` and `DataSource` connection string to generate proper
PigStorage/BinStorage/JsonStorage
