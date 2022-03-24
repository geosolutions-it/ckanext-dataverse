from setuptools import setup, find_packages

version = '1.0.0'

setup(
    name='ckanext-dataverse',
    version=version,
    description="Harvesting data processing plugin for CKAN",
    long_description="""\
    """,
    classifiers=[],
    keywords='',
    author='CKAN',
    author_email='ckan@okfn.org',
    url='https://github.com/geosolutions-it/ckanext-dataverse',
    license='AGPL',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
            # dependencies are specified in pip-requirements.txt
            # instead of here
    ],
    tests_require=[
        'nose',
        'mock',
    ],
    test_suite='nose.collector',
    entry_points="""
        
    """,
    message_extractors={
        'ckanext': [
            ('**.py', 'python', None),
            ('**.js', 'javascript', None),
            ('**/templates/**.html', 'ckan', None),
        ],
    }
)
