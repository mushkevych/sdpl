## TODO

### version +1

1. Simplify use of the relation name in the field resolution:


    C = FILTER A BY A.a == 3; 
    -> 
    C = FILTER A BY a == 3; 

1. Perform `FOREACH ... GENERATE` output for `SCHEMA PROJECTION ( schemaFields )` if it contains `COMPUTE` construct. 
Otherwise the computed fields will be lost. 

### version +2

1. remove keyword `COMPUTE` 

1. Parse `DataSink` and `DataSource` connection string to generate proper
PigStorage/BinStorage/JsonStorage
