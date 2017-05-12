__author__ = 'Bohdan Mushkevych'

import os
import traceback
import unittest

from parser.driver import run_generator
from parser.abstract_lexicon import AbstractLexicon
from parser.pig_lexicon import PigLexicon
from parser.spark_lexicon import SparkLexicon

TESTS_ROOT = os.path.abspath(os.path.dirname(__file__))


def files_with_prefix(prefix, location=TESTS_ROOT):
    files = list()
    for f in os.listdir(location):
        if f.startswith(prefix) and f.endswith('sdpl'):
            files.append(os.path.join(TESTS_ROOT, f))
    return files


def run_success_cases(sdpl_cases, tester, lexicon_class, output_ext):
    assert issubclass(lexicon_class, AbstractLexicon)

    # run all the tests from the nominal tests
    for full_path in sdpl_cases:
        try:
            # locate compiler output - .pig file, and create one if needed
            base_file_path = full_path.replace('.sdpl', output_ext)
            if not os.path.isfile(base_file_path):
                # .pig file does not exist yet - create one
                with open(base_file_path, mode='w') as output_stream:
                    run_generator(full_path, output_stream, lexicon_class)

            # locate compiler output - .out file
            output_file_path = full_path.replace('.sdpl', '{0}.out'.format(output_ext))
            with open(output_file_path, mode='w') as output_stream:
                run_generator(full_path, output_stream, lexicon_class)

            # compare two files - .pig vs .out
            with open(base_file_path, 'r') as base_file, open(output_file_path, 'r') as output_file:
                # compute `set` with elements in either `base_file` or `output_file` but not both
                same = set(base_file).symmetric_difference(output_file)
            same.discard('\n')

            tester.assertEqual(same, set(),
                               'SDPL output for {0} does not match baseline: {1}'.format(full_path, same))
            os.remove(output_file_path)
        except:
            traceback.print_exc()
            tester.assertTrue(False, 'SDPL parser threw exception for {0}'.format(full_path))


def run_failure_cases(sdpl_cases, tester, lexicon_class):
    assert issubclass(lexicon_class, AbstractLexicon)

    for full_path in sdpl_cases:
        null_stream = open(os.devnull, 'w')  # redirect error cases output to /dev/null
        try:
            run_generator(full_path, null_stream, lexicon_class)
            tester.assertTrue(False, 'SDPL parser exception was not expected for {0}'.format(full_path))
        except:
            tester.assertTrue(True, 'expected SDPL parser exception was received for {0}'.format(full_path))
        finally:
            null_stream.close()


class SdplPigUnitTest(unittest.TestCase):
    def __init__(self, methodName='runTest'):
        super(SdplPigUnitTest, self).__init__(methodName)
        self.sdpl_cases = files_with_prefix('snippet')
        self.sdpl_error_cases = files_with_prefix('error_snippet')

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_pig_success_cases(self):
        run_success_cases(self.sdpl_cases, self, PigLexicon, '.pig')

    def test_pig_failure_cases(self):
        run_failure_cases(self.sdpl_error_cases, self, PigLexicon)

    def test_spark_success_cases(self):
        run_success_cases(self.sdpl_cases, self, SparkLexicon, '.pyspark')

    def test_spark_failure_cases(self):
        run_failure_cases(self.sdpl_error_cases, self, SparkLexicon)

if __name__ == '__main__':
    unittest.main()
