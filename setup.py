import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='mongoengine-migrate',
    description='Migrations for MongoEngine ODM inspired by Django',
    version='0.0.3',  # Also pyproject.toml
    author='Igor Derkach',
    author_email='gosha753951@gmail.com',
    url='https://github.com/bdragon300/mongoengine-migrate',
    license='Apache-2.0',
    python_requires='>=3.6',
    packages=setuptools.find_packages(exclude=['tests', 'docs', 'githooks', 'install']),
    package_data={'mongoengine_migrate': ['migration_template.tpl']},
    long_description=long_description,
    long_description_content_type='text/markdown',
    entry_points={"console_scripts": ["mongoengine_migrate=mongoengine_migrate.cli:cli"]},
    keywords=["mongo", "mongodb", "mongoengine", "migrate", "migration"],
    classifiers=[
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Intended Audience :: Developers',
        'Topic :: Database'
    ],
    # Also tox.ini
    install_requires=[
        'mongoengine>=0.16.0',
        'pymongo>=3.0',
        'dictdiffer>=0.7.0',
        'jinja2',
        'click',
        'wrapt',
        'python-dateutil',
        'jsonpath_rw'
    ],
)
