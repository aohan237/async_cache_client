#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import sys

import setuptools
import setuptools.command.test

try:
    import platform
    _pyimp = platform.python_implementation
except (AttributeError, ImportError):
    def _pyimp():
        return 'Python'

NAME = 'cache-client'
PACKAGE = 'cache_client'

E_UNSUPPORTED_PYTHON = '%s 1.0 requires %%s %%s or later!' % (NAME,)

PYIMP = _pyimp()
PY26_OR_LESS = sys.version_info < (2, 7)
PY3 = sys.version_info[0] == 3
PY35_OR_LESS = sys.version_info < (3, 5)
PYPY_VERSION = getattr(sys, 'pypy_version_info', None)
PYPY = PYPY_VERSION is not None
PYPY24_ATLEAST = PYPY_VERSION and PYPY_VERSION >= (2, 4)

if PY35_OR_LESS:
    raise Exception(E_UNSUPPORTED_PYTHON % (PYIMP, '3.5'))

# -*- Classifiers -*-

classes = """
    Development Status :: 5 - Production/Stable
    License :: OSI Approved :: BSD License
    Programming Language :: Python
    Programming Language :: Python :: 3
    Programming Language :: Python :: 3.5
    Programming Language :: Python :: 3.6
    Programming Language :: Python :: 3.7
    Programming Language :: Python :: Implementation :: CPython
    Programming Language :: Python :: Implementation :: PyPy
    Operating System :: OS Independent
    Topic :: Communications
    Topic :: System :: Distributed Computing
    Topic :: Software Development :: Libraries :: Python Modules
"""
classifiers = [s.strip() for s in classes.split('\n') if s]

# -*- Distribution Meta -*-

re_meta = re.compile(r'__(\w+?)__\s*=\s*(.*)')
re_doc = re.compile(r'^"""(.+?)"""')


def add_default(m):
    attr_name, attr_value = m.groups()
    return ((attr_name, attr_value.strip("\"'")),)


def add_doc(m):
    return (('doc', m.groups()[0]),)


pats = {re_meta: add_default,
        re_doc: add_doc}
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, PACKAGE, '__init__.py')) as meta_fh:
    meta = {}
    for line in meta_fh:
        if line.strip() == '# -eof meta-':
            break
        for pattern, handler in pats.items():
            m = pattern.match(line.strip())
            if m:
                meta.update(handler(m))

print(meta)
# -*- Installation Requires -*-


def strip_comments(l):
    return l.split('#', 1)[0].strip()


install_requires = [
    'aiomysql>=0.0.20',
    'aioredis>=1.2.0',
    'pyyaml>=5.1'
]
# -*- Long Description -*-


if os.path.exists('README.md'):
    with open('README.md', 'r') as f:
        long_description = f.read()
else:
    long_description = 'See http://pypi.python.org/pypi/%s' % (NAME,)


setuptools.setup(
    name=NAME,
    packages=setuptools.find_packages(exclude=[
        'ez_setup', 't', 't.*',
    ]),
    version=meta['version'],
    description=meta['doc'],
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords='asyncio cache client',
    author=meta['author'],
    author_email=meta['contact'],
    url=meta['homepage'],
    platforms=['any'],
    license='BSD',
    install_requires=install_requires,
    classifiers=classifiers,
    include_package_data=False,
    zip_safe=False,
)
