from setuptools import find_packages, setup

if __name__ == "__main__":
    setup(
        name="dagster_project",
        packages=find_packages(),
        package_data={"dagster_project": ["dbt_project/*"]},
        install_requires=[
            "dagster",
            "dagster-dbt",
            "pandas",
            "pandas-gbq",
            "pandera",
            "numpy",
            "scipy",
            "dbt-core",
            "dbt-duckdb",
            "dbt-bigquery",
            "dagster-duckdb",
            "dagster-duckdb-pandas",
            "dagster-cloud", 
            "requests",
            "gcsfs",
            "google-cloud"
        ],
        extras_require={"dev": ["dagit", "pytest"]},
    )