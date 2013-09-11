from setuptools import setup, find_packages
import os, sys

execfile('RunnerBean/__version__.py')

setup(name="RunnerBean",
    description='A simple tool for creating long-running Python workers listening for Beanstalk jobs.',
    version=__version__,
    url="https://github.com/wewriteapps/RunnerBean",
    packages=find_packages(exclude="specs"),
    dependency_links = [
        "https://github.com/wewriteapps/resolver/tarball/0.2.1#egg=resolver-0.2.1"
          ],
    install_requires=[
          'resolver>=0.2.0',
          'PyYAML',
          'beanstalkc'
          ],
    )
