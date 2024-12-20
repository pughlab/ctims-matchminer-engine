from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="PMatchEngine",
    version="2.2.3",
    packages=find_packages(),
    package_data={
        'matchengine': ['defaults/**/*', 'defaults/*'],
    },
    data_files=[('pugh-lab', [
        'pugh-lab/plugins/__init__.py',
        'pugh-lab/plugins/clinical_filter.py',
        'pugh-lab/plugins/oncotree_mapping.json',
        'pugh-lab/plugins/query_processor.py',
        'pugh-lab/plugins/query_transformers.py',
        'pugh-lab/plugins/trial_match_document_creator.py',
        'pugh-lab/config.json'])],
    author='Eric Marriott, Ethan Siegel',
    author_email='esiegel@ds.dfci.harvard.edu',
    description='Open source engine for matching cancer patients to precision medicine clinical trials (V2).',
    long_description=long_description,
    entry_points={
        "console_scripts": [
            "matchengine = matchengine.cli:run_cli"
        ]
    },
    install_requires=[
        "python-dateutil==2.8.2",
        "PyYAML==5.4.1; python_version<'3.10'",
        "PyYAML==6.0.1; python_version>='3.10'",
        "pymongo==3.12.0",
        "networkx==2.6.3",
        "motor==2.5.1",
        "pytest~=7.4.0"
    ],
    include_package_data=True,
    python_requires='>=3.7,<3.11',
    # download_url='https://github.com/dfci/matchengine-V2/archive/2.0.0.tar.gz',
    download_url='https://github.com/pughlab/pmatchengine-pugh-lab/archive/mickey-qa-deploy.tar.gz',

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
