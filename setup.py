import io
import os

from setuptools import find_packages, setup


def read_file(filename):
    with open(filename) as fp:
        return fp.read().strip()


def read_requirements(filename):
    return [
        line.strip() for line in read_file(filename).splitlines()
        if not line.startswith("#")
    ]


NAME = 'pyelastices'
DESCRIPTION = ("✨ Awesome elasticsearch framework that shines ✨")

here = os.path.abspath(os.path.dirname(__file__))


def get_about(author, url, email):
    """Return package about information
    """
    about = {}
    about["__email__"] = email
    about["__url__"] = url
    about["__author__"] = author

    with open(os.path.join(here, NAME, "__version__.py")) as f:
        exec(f.read(), about)
    return about


def get_long_description():
    """Return the README
    """
    try:
        with io.open(os.path.join(here, "README.md"), encoding="utf-8") as f:
            long_description = "\n" + f.read()
    except FileNotFoundError:
        long_description = DESCRIPTION
    return long_description


about = get_about(
    author="szj",
    url="https://github.com/szj2ys/pyelastices",
    email="szj2ys@qq.com",
)

setup(
    name=NAME,
    version=about["__version__"],
    author=about['__author__'],
    author_email=about['__email__'],
    url=about['__url__'],
    description=DESCRIPTION,
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    python_requires=">=3.5.0",
    packages=find_packages(exclude=["examples"]),
    package_data={NAME: ["*"]},
    data_files=[("", ["LICENSE"])],
    install_requires=read_requirements("requirements.txt"),
    include_package_data=True,
    entry_points={
        "console_scripts": [
            "pyelastices=pyelastices.cmdline:run",
        ],
    },
    license="MIT",
    # https://pypi.org/classifiers/
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
    ],
    zip_safe=False,
)
