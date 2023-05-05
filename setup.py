import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.production.txt", "r") as fh:
    install_requires = fh.read()

with open("VERSION", "r") as fh:
    version = fh.read().strip()

setuptools.setup(
    name="mela",
    version=version,
    author="Daniyar Supiyev",
    author_email="undead.thunderbird@gmail.com",
    description="Let's make microservice development fun again!",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/alem-research/mela",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Development Status :: 4 - Beta",
        "Framework :: AsyncIO",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
        ""
    ],
    python_requires='>=3.11',
    install_requires=install_requires
)
