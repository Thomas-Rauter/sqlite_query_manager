from setuptools import setup, find_packages

setup(
    name="sqlite_manager",
    version="0.1.0",
    description="A package for managing SQLite operations",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Thomas Rauter",
    author_email="rauterthomas0@gmail.com",
    url="https://github.com/Thomas-Rauter/sqlite_query_manager",
    license="MIT",
    packages=find_packages(),
    install_requires=[
        "pandas>=2.0.0",
        "tqdm>=4.5.0",
    ],
    python_requires=">=3.7",             # Minimum Python version
    classifiers=[                        # Metadata for PyPI
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
