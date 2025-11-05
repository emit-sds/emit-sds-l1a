"""
This is the setup file for managing the emit_sds_l1a package

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="emit_sds_l1a",
    version="1.6.0",
    author="Winston Olson-Duvall",
    author_email="winston.olson-duvall@jpl.nasa.gov",
    description="""
        L1A PGEs for EMIT data processing, including depacketization and reassembly or reformatting of raw data
        """,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.jpl.nasa.gov/emit-sds/emit-sds-l1a",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
    install_requires=[
        "ait-core==2.5.2",
        "h5netcdf>=1.7.3",
        "matplotlib>=3.10.7",
        "numpy>=2.2.6",
        "sortedcontainers>=2.4.0",
        "spectral>=0.24"
    ],
    extras_require={
        "dev": [
            "pycodestyle>=2.6.0",
            "pytest>=6.2.1",
            "pytest-cov>=2.10.1"
        ]
    }
)
