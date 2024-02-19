import glob
from shutil import rmtree

import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()


class CleanCommand(setuptools.Command):
    """ Custom clean command to tidy up the project root. """
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        rmtree('build', ignore_errors=True)
        rmtree('dist', ignore_errors=True)
        for file in glob.glob('*.egg-info'):
            rmtree(file)


setuptools.setup(
    cmdclass={'clean': CleanCommand},
    name='filecloudsync',
    version='0.0.1',
    url='',
    license='',
    author='José Manuel Gómez Soriano',
    author_email='jmgomez.soriano@gmail.com',
    long_description=long_description,
    long_description_content_type='text/markdown',
    description='Module for MLOps and IA Altia teams',
    packages=setuptools.find_packages(exclude=["test"]),
    package_dir={'filecloudsync': 'filecloudsync'},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    requirements=[
        'PyYAML>=5.4.1,<7',
        'boto3>=1.34.34,<2',
        'mysmallutils>=2.0.15,<3',
        'watchdog>=4.0.0,4.1'
    ],
    python_requires='>=3.7,<4'
)
