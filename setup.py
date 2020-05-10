import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='mongoengine-migrate',
    description='Migrations for MongoEngine ODM inspired by Django',
    version='0.1a1.dev1',
    author='Igor Derkach',
    author_email='gosha753951@gmail.com',
    url='https://github.com/bdragon300/mongoengine-migrate',
    license='Apache-2.0',
    python_requires='>=3',
    packages=setuptools.find_packages(exclude=['tests']),
    long_description=long_description,
    long_description_content_type='text/markdown',
    scripts=['bin/mongoengine-migrate.py'],
    classifiers=[
        'Programming Language :: Python :: 3',
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Intended Audience :: Developers',
        'Topic :: Database'
    ],
    install_requires=[
        'mongoengine>=0.20.0',
        'pymongo>=3.9',
        'dictdiffer>=0.8.1',
        'jinja2',
        'click',
        'wrapt'
    ],
    tests_require=['pytest'],
    setup_requires=['pytest-runner'],
)
