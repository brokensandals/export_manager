import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="export_manager", # Replace with your own username
    version="0.0.1",
    author="Jacob Williams",
    author_email="jacobaw@gmail.com",
    description="A tool for managing automated exports of personal data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/brokensandals/export_manager",
    packages=setuptools.find_packages('src'),
    package_dir={'':'src'},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'gitdb>=4',
        'GitPython>=3.1',
        'toml>=0.10',
    ],
    python_requires='>=3.7',
)