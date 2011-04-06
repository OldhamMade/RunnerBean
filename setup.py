from setuptools import setup, find_packages
import os, sys

execfile('RunnerBean/__version__.py')

setup(name="RunnerBean",
      description=open(os.path.join(os.path.dirname(__file__), 'README.rst')).read(),
      version=__version__,
      url="http://unpluggd.github.com/RunnerBean",
      packages=find_packages(exclude="specs"),
      )
      
