# SDPL
schema driven processing language

[![PyPI version](https://img.shields.io/pypi/v/synergy_scheduler.svg)](https://pypi.python.org/pypi/sdpl)
[![Build Status](https://api.bitbucket.org/2.0/repositories/{owner}/{repo_slug}/commit/{revision}/statuses/build/{key})](https://travis-ci.org/mushkevych/sdpl/)

SDPL introduces data schema to major data processing languages
such as Apache Pig, Spark and Hive. SDPL supports generic operations such as
LOAD, STORE, JOIN, PROJECT, while complex transformation and fine-tuning
are intended to be performed in the target language via quotation

# Repository

Main repository: https://bitbucket.org/mushkevych/sdpl
Mirror: https://github.com/mushkevych/sdpl

# Installation

- Python3.5

- antlr4 package
    
    `sudo apt-get install antlr4`
    
- antlr4-python3-runtime

    ` $> pip install antlr4-python3-runtime `

- PyYAMP

    ` $> pip install PyYAML `

# Compile parser and lexer out of the .g4 grammar

    `$> antlr4 -Dlanguage=Python3 quotes.g4`
    `$> antlr4 -Dlanguage=Python3 sdpl.g4`


# run example
```
$> python3 sdpl.py pig -i tests/snippet_1.sdpl
```
