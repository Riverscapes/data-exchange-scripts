from setuptools import setup, find_packages

setup(
    name='rsapi',
    version='0.1.0',
    author='Matt Reimer',
    author_email='matt@northarrowresearch.com',
    description='A description of your project',
    long_description=open('README.md', encoding='utf8').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/yourusername/riverscapes',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        # Add your project's dependencies here
        'requests',
        'graphql-core',
        # ...
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.9',
)
