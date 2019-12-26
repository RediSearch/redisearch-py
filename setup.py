
#!/usr/bin/env python
from setuptools import setup, find_packages
import io

def read_all(f):
    with io.open(f, encoding="utf-8") as I:
        return I.read()

requirements = map(str.strip, open("requirements.txt").readlines())

setup(
    name='redisearch',
    version='0.8.1',
    description='RedisSearch Python Client',
    long_description=read_all("README.md"),
    long_description_content_type='text/markdown',
    url='http://github.com/RedisLabs/redisearch-py',
    packages=find_packages(),
    install_requires=['redis', 'hiredis', 'rmtest'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database',
        'Topic :: Software Development :: Testing'
    ],
    keywords='Redis Search Extension',
    author='RedisLabs',
    author_email='oss@redislabs.com'    
)
