## TODO

### version +1

1. Simplify use of the relation name in the field resolution:


    C = FILTER A BY A.a == 3; 
    -> 
    C = FILTER A BY a == 3;   
      
    C = GROUP A BY A.a; 
    -> 
    C = GROUP A BY a;  
      
    C = ORDER A BY A.a; 
    -> 
    C = ORDER A BY a; 

