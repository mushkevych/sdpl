# SDPL
schema driven processing language

[![PyPI version](https://img.shields.io/pypi/v/sdpl.svg)](https://pypi.python.org/pypi/sdpl)

SDPL introduces data schema to major data processing languages
such as Apache Pig, Spark and Hive. SDPL supports generic operations such as
`LOAD`, `STORE`, `JOIN`, `PROJECT`, while complex transformation and fine-tuning
are performed in the target language via quotation.

SDPL links 3 artifacts:
 
 * DataRepository file describes data source and credentials needed to access it
 * Schema file describes the data 
 * Source code describes what data to load and transformation to apply  

Supported target languages are Apache Pig and Spark; DataRepository is a short YAML file; 
Schema could be recorded in YAML, AVRO or Protobuf   


# Repository

Main repository: https://bitbucket.org/mushkevych/sdpl  
Mirror: https://github.com/mushkevych/sdpl  

# Installation

- Python3.5+

- antlr4 package
    
    `sudo apt-get install antlr4`
    
- antlr4-python3-runtime

    ` $> pip install antlr4-python3-runtime `

- PyYAMP

    ` $> pip install PyYAML `

# Compile parser and lexer out of the .g4 grammar

    `$> antlr4 -Dlanguage=Python3 sdpl.g4`


# run example
```bash
$> python3 sdpl.py pig -i tests/snippet_1.sdpl
```
