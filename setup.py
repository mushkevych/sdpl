from distutils.core import setup

setup(name='sdpl',
      version='0.1',
      description='Schema Driven Processing Language',
      author='Bohdan Mushkevych',
      author_email='mushkevych@gmail.com',
      url='https://github.com/mushkevych/sdpl',
      packages=['grammar', 'parser', 'schema'],
      package_data={'grammar': ['*']},
      long_description='SDPL introduces data schema to major data processing languages '
                       'such as Apache Pig, Spark and Hive. SDPL supports generic operations such as '
                       'LOAD, STORE, JOIN, PROJECT, while complex transformation and fine-tuning '
                       'are intended to be performed in the target language via quotation',
      license='BSD 3-Clause License',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Environment :: Console',
          'Intended Audience :: Developers',
          'Intended Audience :: Information Technology',
          'Intended Audience :: System Administrators',
          'License :: OSI Approved :: BSD License',
          'Operating System :: POSIX',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'Topic :: Software Development :: Code Generators',
      ],
      install_requires=['antlr4-python3-runtime', 'PyYAML', 'xmlrunner', 'pylint']
      )
