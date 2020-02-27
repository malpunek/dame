import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dame", # Replace with your own username
    version="0.0.1",
    author="Stanisław Morawski",
    author_email="stas.morawski@gmail.com",
    description="Manage your dataflows seamlessly",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/malpunek/dame",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 1 - Planning",
        "Topic :: Software Development"
    ],
    python_requires='>=3.6',
    install_requires=['numpy', 'peewee>=3'],
)
