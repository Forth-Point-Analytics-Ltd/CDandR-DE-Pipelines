import json
from typing import Dict, List

import yaml


def load_json(path: str) -> Dict[str, str]:
    with open(path) as f:
        return json.load(f)


def load_yaml(path: str) -> Dict[str, str]:
    with open(path, "r") as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as err:
            print(err)
    return config


def configure_cluster_spark_Settings(
    job_cluster_path: str,
    cluster_name: str,
    spark_config_vars: Dict[str, str],
    spark_env_vars: Dict[str, str],
) -> Dict[str, str]:
    job_cluster_settings = load_json(job_cluster_path)
    for job_cluster_setting in job_cluster_settings:
        job_cluster_setting["job_cluster_key"] = cluster_name
        if spark_config_vars is not None:
            for spark_env_key, spark_env_val in spark_config_vars.items():
                job_cluster_setting["new_cluster"]["spark_conf"][
                    spark_env_key
                ] = spark_env_val
        if spark_env_vars is not None:
            for env_key, env_val in spark_env_vars.items():
                job_cluster_setting["new_cluster"]["spark_env_vars"][
                    env_key
                ] = env_val
    return job_cluster_settings


def create_cluster_pypi_libraries_config(requirements_path: str) -> List[str]:
    libraries = []
    with open(requirements_path) as packages:
        for package in packages.readlines():
            pypi_package = {"pypi": {"package": package.replace("\n", "")}}
            libraries.append(pypi_package)
    return libraries


def configure_workflow_name(
    git_branch: str,
    environment: str,
    workflow: Dict[str, str],
    workflow_name: str = None,
) -> str:
    task_name_branch = (
        f"_{git_branch}" if git_branch and git_branch != "main" else ""
    )
    workflow_name = (
        workflow_name if workflow_name else f"{workflow['settings']['name']}"
    )
    return f"{workflow_name}{task_name_branch}_{environment}"
