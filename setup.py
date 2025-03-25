from setuptools import setup, find_packages

setup(
    name="voter_framework",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pandas>=1.3.0",
        "numpy>=1.20.0",
        "pyyaml>=5.4.0",
    ],
    python_requires=">=3.9",
    entry_points={
        "console_scripts": [
            "onboard-state=voter_framework.cli.onboard_state:main",
            "import-to-sqlite=voter_framework.cli.import_to_sqlite:main",
        ],
    },
) 