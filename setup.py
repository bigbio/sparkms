import io

from setuptools import setup, find_packages


def readme():
  with open('README.md') as f:
    return f.read()


setup(name='sparkms',
      version='0.0.1',
      description='Python tools for proteomics data analysis in Spark',
      url='http://github.com/bigbio/sparkms',
      long_description=readme(),
      long_description_content_type='text/markdown',
      author='PgAtk Team',
      author_email='ypriverol@gmail.com',
      license='LICENSE.txt',
      include_package_data=True,
      install_requires=[],
      scripts=['sparkms/sparkmscli.py'],
      packages=find_packages(),
      entry_points={
        'console_scripts': [
          'sparkms = sparkms.sparkmscli:main'
        ]},
      package_data={'sparkms': ['config/*.yaml', 'config/*.json']},
      zip_safe=False)
