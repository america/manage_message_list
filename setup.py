#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='manage_message_list',
    version='0.0.1',
    url='https://github.com/america/Python',
    author='Takashi Haga',
    author_email='dreamers.ball66@gmail.com',
    license='MIT',
    packages=find_packages(),
    include_package_data=True,
    install_requires=required,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Programming Language :: Python :: 3.5',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Topic :: Database',
    ],

)
