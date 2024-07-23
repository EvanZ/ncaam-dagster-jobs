from setuptools import find_packages, setup

setup(
    name="dagster_jobs",
    packages=find_packages(exclude=["dagster_jobs_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "loguru",
     ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
