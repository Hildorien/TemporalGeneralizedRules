import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="TemporalGeneralizedRules",                     # This is the name of the package
    version="1.0.2",                        # The initial release version
    author="Ignacio Fernandez y Emiliano Galimberti",                     # Full name of the author
    description="Algorithms for association Rule mining",
    long_description=long_description,      # Long description read from the the readme file
    long_description_content_type="text/markdown",
    license='MIT',
    packages=setuptools.find_packages(),    # List of all python modules to be installed
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],                                      # Information to filter the project on PyPi website
    python_requires='>=3.8',                # Minimum version requirement of the package
    install_requires=[]                     # Install other dependencies if any
)