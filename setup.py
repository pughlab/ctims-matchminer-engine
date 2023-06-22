from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="MatchEngine V2",
    version="2.2.0",
    packages=[
        "matchengine",
        "matchengine.internals",
        "matchengine.internals.database_connectivity",
        "matchengine.internals.plugin_helpers",
        "matchengine.internals.typing",
        "matchengine.internals.utilities",
        "matchengine.plugins"
    ],
    author='Eric Marriott, Ethan Siegel',
    author_email='esiegel@ds.dfci.harvard.edu',
    description='Open source engine for matching cancer patients to precision medicine clinical trials (V2).',
    long_description=long_description,
    entrypoints={
        "console-scripts": [
            "matchengine =  matchengine.main"
        ]
    },
    install_requires=[
        "python-dateutil==2.8.2",
        "PyYAML==5.4.1",
        "pymongo==3.12.0",
        "networkx==2.6.3",
        "motor==2.0.0",
        "pytest==7.2.1"
    ],
    include_package_data=True,
    python_requires='>=3.7',
    download_url='https://github.com/dfci/matchengine-V2/archive/2.0.0.tar.gz',
    classifiers=[
        "Environment :: Console",
        "Intended Audience :: Healthcare Industry",
        "Intended Audience :: Science/Research",
        "Operating System :: MacOS",
        "Operating System :: POSIX",
        "Operating System :: POSIX :: Linux",
        "Operating System :: POSIX :: BSD",
        "Operating System :: Unix",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Medical Science Apps.",
        "Topic :: Utilities",
        "Typing :: Typed"
    ]
)
