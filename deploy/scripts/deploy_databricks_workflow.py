import argparse
import os
from typing import Dict

from databricks_common import DatabricksAPIClient
from dotenv import load_dotenv

from utils import (
    configure_cluster_spark_Settings,
    create_cluster_pypi_libraries_config,
    configure_workflow_name,
    load_json,
    load_yaml,
)

load_dotenv()


def update_workflow(
    databricks_api: DatabricksAPIClient,
    cluster_name: str,
    workflow_path: str,
    requirements_path: str,
    job_cluster_path: str,
    spark_env_vars: Dict[str, str] = None,
    spark_config_vars: Dict[str, str] = None,
    environment: str = None,
    git_branch: str = None,
    git_tag: str = None,
    git_hash: str = "",
    workflow_settings: Dict[str, str] = None,
    cluster_settings: Dict[str, str] = None,
    **kwargs
) -> int:
    """Creates new workflow or updates an existing workflow from a job in Databricks

    Args:
        databricks_api (DatabricksAPIClient): The DataBricksAPIClient to make requests.
        cluster_name (str): The name of the cluster.
        workflow_path (str): The local directory path where your cluster config is located.
        packages_path (str): The local directory path where cluster packages are located.
        job_cluster_path (str): The local directory path where the job cluster  config is located.
        spark_env_vars (Dict[str,str]): Environment variables for the db spark cluster.
        spark_config_vars (Dict[str, str]): Spark configuration variables for the db spark cluster.
        environment (str): The environment where the code is executed.
        git_branch(str): The git branch associated with code to tun.
        git_tag(str): The git tag associated with a release.
        git_hash(str): The git hash associated with the code.

    Returns:
        int: The workflow job id

    Examples:
        >>> update_workflow(
            databricks_api = DatabricksAPIClient(
                url = "https://databricks_workspace_url.com"
                token = "databricks_personal_access_token"
            ),
            cluster_name = "cluster_name",
            workflow_path = "path/pipelines/workflow.json",
            packages_path = "path/packages/requirements.txt",
            job_cluster_path = "path/jobs/job_cluster.json",
            spark_env_vars = {"ENV":"REDACTED"},
            spark_config_vars = {"spark.config": "REDACTED"},
            environment = "dev",
            git_branch = "dev_branch",
            git_tag = "git_tag",
            git_hash = "git_hash",
        )
        76
    """
    workflow = load_json(workflow_path)
    job_cluster_settings = configure_cluster_spark_Settings(
        job_cluster_path, cluster_name, spark_config_vars, spark_env_vars
    )

    for cluster_setting in cluster_settings.keys():
        job_cluster_settings[0]["new_cluster"][
            cluster_setting
        ] = cluster_settings[cluster_setting]
    print(workflow_settings)
    for workflow_setting in workflow_settings:
        workflow["settings"][workflow_setting] = workflow_settings[
            workflow_setting
        ]
    current_tasks = workflow["settings"]["tasks"]
    updated_tasks = []
    libraries = create_cluster_pypi_libraries_config(requirements_path)
    for task in current_tasks:
        task["job_cluster_key"] = cluster_name
        task["libraries"] = libraries
        task["notebook_task"]["base_parameters"]["env_prefix"] = environment
        task["notebook_task"]["base_parameters"]["git_hash"] = git_hash
        updated_tasks.append(task)
    workflow["settings"]["tasks"] = updated_tasks
    workflow["settings"]["job_clusters"] = job_cluster_settings

    if git_branch is not None:
        workflow["settings"]["git_source"]["git_branch"] = git_branch
    elif git_tag is not None:
        workflow["settings"]["git_source"]["git_tag"] = git_tag
    workflow_name = configure_workflow_name(git_branch, environment, workflow)
    if environment is not None:
        workflow["settings"]["name"] = workflow_name

    job_id = databricks_api.get_job_id_from_name(workflow["settings"]["name"])
    print(workflow)
    if job_id is None:
        return databricks_api.create_workflow(workflow)
    return databricks_api.update_workflow(job_id, workflow)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    required_named = parser.add_argument_group("required named arguments")
    required_named.add_argument("-c", "--config-file", required=True)

    optional_named = parser.add_argument_group("optional named arguments")
    optional_named.add_argument("-gh", "--git-hash", nargs="?", default="")
    optional_named.add_argument(
        "-g", "--github-branch-path", nargs="?", default=None
    )
    optional_named.add_argument("-gt", "--github-tag", nargs="?", default=None)
    optional_named.add_argument(
        "-w", "--workflow_path", nargs="?", default=None
    )
    args = parser.parse_args()
    yaml_path = args.config_file

    config = load_yaml(yaml_path)
    databricks_api = DatabricksAPIClient(
        config["databricks_url"],
        os.environ.get(config["databricks_token_key"]),
    )
    config["workflow_path"] = (
        args.workflow_path if args.workflow_path else config["workflow_path"]
    )
    update_workflow(
        databricks_api=databricks_api,
        git_hash=args.git_hash,
        git_branch=args.github_branch_path,
        git_tag=args.github_tag,
        **config,
    )
