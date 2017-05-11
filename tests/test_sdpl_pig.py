__author__ = 'Bohdan Mushkevych'

import os
import traceback
import unittest

from parser.driver import run_generator
from parser.pig_lexicon import PigLexicon

TESTS_ROOT = os.path.abspath(os.path.dirname(__file__))


def files_with_prefix(prefix, location=TESTS_ROOT):
    files = list()
    for f in os.listdir(location):
        if f.startswith(prefix) and f.endswith('sdpl'):
            files.append(os.path.join(TESTS_ROOT, f))
    return files


class SdplPigUnitTest(unittest.TestCase):
    def __init__(self, methodName='runTest'):
        super(SdplPigUnitTest, self).__init__(methodName)
        self.sdpl_cases = files_with_prefix('snippet')
        self.sdpl_error_cases = files_with_prefix('error_snippet')

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_runner(self):
        # run all the tests from the nominal tests
        for full_path in self.sdpl_cases:
            try:
                # locate compiler output - .pig file, and create one if needed
                base_file_path = full_path.replace('.sdpl', '.pig')
                if not os.path.isfile(base_file_path):
                    # .pig file does not exist yet - create one
                    with open(base_file_path, mode='w') as output_stream:
                        run_generator(full_path, output_stream, PigLexicon)

                # locate compiler output - .out file
                output_file_path = full_path.replace('.sdpl', '.out')
                with open(output_file_path, mode='w') as output_stream:
                    run_generator(full_path, output_stream, PigLexicon)

                # compare two files - .pig vs .out
                with open(base_file_path, 'r') as base_file, open(output_file_path, 'r') as output_file:
                    # compute `set` with elements in either `base_file` or `output_file` but not both
                    same = set(base_file).symmetric_difference(output_file)
                same.discard('\n')

                self.assertEqual(same, set(),
                                 'SDPL output for {0} does not match baseline: {1}'.format(full_path, same))
                os.remove(output_file_path)
            except:
                traceback.print_exc()
                self.assertTrue(False, 'SDPL parser threw exception for {0}'.format(full_path))

        for full_path in self.sdpl_error_cases:
            null_stream = open(os.devnull, 'w')  # redirect error cases output to /dev/null
            try:
                run_generator(full_path, null_stream, PigLexicon)
                self.assertTrue(False, 'SDPL parser exception was not expected for {0}'.format(full_path))
            except:
                self.assertTrue(True, 'expected SDPL parser exception was received for {0}'.format(full_path))
            finally:
                null_stream.close()

if __name__ == '__main__':
    unittest.main()
