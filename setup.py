from setuptools import setup, find_packages

setup(
    name='pydex',
    version='0.1.0',
    author='Matt Reimer',
    author_email='matt@northarrowresearch.com',
    description='Client scripts for the Riverscapes Data Exchange API',
    long_description=open('README.md', encoding='utf8').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/Riverscapes/riverscapes-api',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'graphql-core==3.2.6',
        'inquirer==3.4.0',
        'lxml==5.3.1',
        'python-dateutil==2.9.0.post0',
        'requests==2.32.3',
        'rsxml==2.0.6',
        'semver==3.0.4',
        'setuptools==76.0.0',
        'six==1.17.0',
        'termcolor==2.5.0',
        'urllib3==2.3.0',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.9',
)
