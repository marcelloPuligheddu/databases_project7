from setuptools import setup
from setuptools import find_packages

exec(open("src/versions.py").read())

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name=__name__,
      version=__version__,
      packages=find_packages(),
      description=__description__,
      long_description=long_description,
      long_description_content_type="text/markdown",
      url=__url__,
      author=__author__,
      author_email=__author_email__,
      license=__license__,
      include_package_data=True,
      install_requires=[
          'pandas',
          'setuptools',
      ],
      python_requires='>=3.6, <4')
