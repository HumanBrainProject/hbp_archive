
from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='hbp_archive',
    version='0.9.1',
    description='A high-level API for interacting with the Human Brain Project archival storage at CSCS',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/apdavison/hbp_archive',
    author='Andrew P. Davison and Shailesh Appukuttan, CNRS',
    author_email='andrew.davison@unic.cnrs-gif.fr',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
    ],
    keywords='swift hbp cscs data',
    py_modules=["hbp_archive"],
    install_requires=['lxml',
                      'keystoneauth1',
                      'python-keystoneclient',
                      'python-swiftclient',
                      'pathlib2;python_version<"3"',]
)
