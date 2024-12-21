from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt") as f:
    required = f.read().splitlines()

setup(
    name="standardizex",
    version="0.0.1",
    description="""
    """,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Pallavi Sinha",
    packages=find_packages(include=["standardizex", "standardizex.*"]),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=required,
    include_package_data=True,
    package_data={
        "standardizex": ["config/templates/json/*.json"],
    },
    license="MIT",
    url="https://github.com/Pallavi-Sinha-12/standardizex",
)
