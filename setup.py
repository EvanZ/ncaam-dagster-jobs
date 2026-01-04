from setuptools import find_packages, setup

setup(
    name="dagster_jobs",
    packages=find_packages(exclude=["dagster_jobs_tests"]),
    install_requires=[
        "dagster==1.12.0",
        "dagster-cloud==1.12.0",
        "dagster-duckdb",
        "pandas",
        "fasteners",
        "requests",
        "sklearn",
        "jinja2"
     ],
    extras_require={"dev": ["dagster-webserver==1.12.0", "pytest"]},
)
