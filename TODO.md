## TODO

### version +1

1. Add `COMPUTE` construct 

1. Simplify use of the relation name in the field resolution:


    C = FILTER A BY A.a == 3; 
    -> 
    C = FILTER A BY a == 3; 

### version +2

1. remove keyword `COMPUTE` 

1. Parse DataSink and DataSource connection string to generate proper
PigStorage/BinStorage/JsonStorage
