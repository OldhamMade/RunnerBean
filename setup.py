from setuptools import setup, find_packages
import os, sys

execfile('RunnerBean/__version__.py')

setup(name="RunnerBean",
    description=open(os.path.join(os.path.dirname(__file__), 'README.rst')).read(),
    version=__version__,
    url="http://github.com/unpluggd/RunnerBean",
    packages=find_packages(exclude="specs"),
    dependency_links = [
        "https://github.com/unpluggd/resolver/tarball/0.2.1#egg=resolver-0.2.1"
    ],
    install_requires=['simplejson', 'resolver>=0.2.0', 'PyYAML', 'beanstalkc'],
    )
