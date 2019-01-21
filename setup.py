
#!/usr/bin/env python
from setuptools import setup, find_packages

install_require = ['redis', 'hiredis']

test_require = ['rmtest']

setup(
    name='redisearch',
    version='0.7.1',

    description='RedisSearch Python Client',
    url='http://github.com/RedisLabs/redisearch-py',
    packages=find_packages(),
    install_requires=install_requires,
    test_require=test_require,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database',
        'Topic :: Software Development :: Testing'
    ]
)
