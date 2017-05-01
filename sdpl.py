#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Bohdan Mushkevych'

import argparse
import sys
from os import path

from parser.projection import Schema
from schema.io import load
from parser.driver import run_generator
from parser.pig_generator import PigGenerator
from parser.postresql_schema import compose_ddl

PROJECT_ROOT = path.abspath(path.dirname(__file__))


def init_parser():
    main_parser = argparse.ArgumentParser(prog='sdpl.py')
    subparsers = main_parser.add_subparsers(title='sub-commands', description='list of available sub-commands')

    pig_parser = subparsers.add_parser('pig', help='compile given SDPL file into Apache Pig')
    pig_parser.add_argument('-i', '--infile', action='store', help='SDPL input file')
    pig_parser.add_argument('-o', '--outfile', action='store', help='Apache Pig output file')
    pig_parser.set_defaults(func=compile_pig)

    postgresql_parser = subparsers.add_parser('postgresql',
                                              help='compile given SCHEMA file into PostgreSql *CREATE TABLE* SQL')
    postgresql_parser.add_argument('-s', '--schema', action='store', help='SCHEMA input file')
    postgresql_parser.add_argument('-r', '--repo', action='store', help='RDBMS repository')
    postgresql_parser.add_argument('-t', '--table', action='store',
                                   help='fully qualified table name in format db_schema.table_name')
    postgresql_parser.add_argument('-o', '--outfile', action='store', help='SQL output file')
    postgresql_parser.set_defaults(func=compile_postgresql)

    test_parser = subparsers.add_parser('test', help='run unit tests from the settings.test_cases list')
    test_parser.add_argument('-o', '--outfile', action='store', help='save report results into a file')
    test_parser.set_defaults(func=run_tests)
    test_group = test_parser.add_mutually_exclusive_group()
    test_group.add_argument('-x', '--xunit', action='store_true',
                            help='measure coverage during unit tests execution')
    test_group.add_argument('-p', '--pylint', action='store_true', help='run pylint on the project')

    return main_parser


def compile_pig(parser_args):
    if not parser_args.infile:
        print('ERROR: Input file is missing\n')
        parser_args.parser.parse_args(['pig', '-h'])
        exit(1)

    if not parser_args.outfile:
        print('No output file specified. Using stdout.')
        output_stream = sys.stdout
    else:
        output_stream = open(parser_args.outfile, 'w')

    run_generator(parser_args.infile, output_stream, PigGenerator)


def compile_postgresql(parser_args):
    if not parser_args.schema or not parser_args.repo:
        print('ERROR: Input is incomplete\n')
        parser_args.parser.parse_args(['postgresql', '-h'])
        exit(1)

    if not parser_args.outfile:
        print('No output file specified. Using stdout.')
        output_stream = sys.stdout
    else:
        output_stream = open(parser_args.outfile, 'w')

    schema = load(parser_args.schema)
    data_repo = load(parser_args.repo)
    assert isinstance(schema, Schema)
    output_stream.write(compose_ddl(schema, schema.version, data_repo))


def load_all_tests():
    import unittest
    from settings import test_cases

    return unittest.defaultTestLoader.loadTestsFromNames(test_cases)


def run_tests(parser_args):
    import unittest
    import logging
    import settings
    settings.enable_test_mode()

    def unittest_main(test_runner=None):
        try:
            argv = [sys.argv[0]]
            if parser_args.extra_parameters:
                argv += parser_args.extra_parameters
            else:
                # workaround to avoid full unit test discovery
                # and limit test suite to settings.test_cases
                # argv.append('__main__.load_all_tests')
                argv.append('__main__.load_all_tests')

            unittest.main(module=None, argv=argv, testRunner=test_runner)
        except SystemExit as e:
            if e.code == 0:
                logging.info('PASS')
            else:
                logging.error('FAIL')
                raise

    if parser_args.pylint:
        from pylint import lint
        from pylint.reporters.text import ParseableTextReporter

        output = sys.stdout
        if parser_args.outfile:
            output = open(parser_args.outfile, 'w')

        config = "--rcfile=" + path.join(PROJECT_ROOT, 'pylint.rc')
        lint.Run([config] + settings.testable_modules,
                 reporter=ParseableTextReporter(output=output), exit=False)

    elif parser_args.xunit:
        import xmlrunner

        output = 'reports'
        if parser_args.outfile:
            output = parser_args.outfile
        unittest_main(xmlrunner.XMLTestRunner(output=output))

    else:
        unittest_main(None)


if __name__ == '__main__':
    parser = init_parser()
    parser_namespace, extra_parameters = parser.parse_known_args()
    parser_namespace.extra_parameters = extra_parameters
    parser_namespace.parser = parser

    if not hasattr(parser_namespace, 'func'):
        parser.print_help()
        exit(1)

    # calling a function associated with the sub-command in *parser.set_defaults(func=...)*
    parser_namespace.func(parser_namespace)
