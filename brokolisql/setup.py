from setuptools import setup, find_packages

setup(
    name="brokolisql",
    version="0.2.0",
    packages=find_packages(),
    install_requires=[
        'pandas>=1.0.0',
        'numpy>=1.18.0',
        'tqdm>=4.45.0',
        'openpyxl>=3.0.0',  # for Excel support
        'pyyaml>=5.1',      # for YAML config files
    ],
    entry_points={
        'console_scripts': [
            'brokolisql=brokolisql.cli:main',
        ],
    },
    author="Abel Eduardo Mondlane",
    author_email="abeleduardohc12@gmail.com",
    description="Convert various file formats to SQL commands for different database systems",
    keywords="sql, csv, excel, database, convert, etl",
    url="https://github.com/hc12r/brokolisql",
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Database',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
)