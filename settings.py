ENVIRONMENT = '%ENVIRONMENT%'

# folder locations, connection properties etc
settings = dict(
    debug=False,                # if True, logger.setLevel is set to DEBUG. Otherwise to INFO

    under_test=False            # marks execution of the Unit Tests
)

# Update current dict with the environment-specific settings
try:
    overrides = __import__('settings_' + ENVIRONMENT)
except:
    overrides = __import__('settings_dev')
settings.update(overrides.settings)


# Modules to test and verify (pylint/pep8)
testable_modules = [
    'parser',
    'schema',
]

test_cases = [
    'tests.test_protobuf_schema',
    'tests.test_avro_schema',
    'tests.test_sdpl_schema',
    'tests.test_sdpl_generator',
]


def enable_test_mode():
    if settings['under_test']:
        # test mode is already enabled
        return

    test_settings = dict(
        debug=True,
        under_test=True,
    )
    settings.update(test_settings)
