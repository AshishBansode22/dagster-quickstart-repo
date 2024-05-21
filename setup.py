from setuptools import find_packages, setup

setup(
    name="new_etl_project",
    packages=find_packages(exclude=["new_etl_project_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
