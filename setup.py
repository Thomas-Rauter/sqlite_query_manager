from setuptools import setup, find_packages

setup(
    name="query_manager",
    version="0.1.0",
    description="A package for managing SQL queries on SQLite databases",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Thomas Rauter",
    author_email="rauterthomas0@gmail.com",
    url="https://github.com/Thomas-Rauter/sqlite_query_manager",
    license="MIT",
    packages=find_packages(),
    install_requires=[
        "pandas>=1.3.0",
        "sqlite3>=2.6.0",
        "numpy>=1.20.0",
        "logging>=0.5.1.2",
        "pathlib>=1.0.1"
    ],
    python_requires=">=3.7",             # Minimum Python version
    classifiers=[                        # Metadata for PyPI
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
