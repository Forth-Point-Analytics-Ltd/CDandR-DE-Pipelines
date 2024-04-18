import argparse
import os
import time

from databricks_common import DatabricksAPIClient
from dotenv import load_dotenv

from utils import configure_workflow_name, load_json, load_yaml

load_dotenv()


def trigger_workflow(
    databricks_api: DatabricksAPIClient, github_branch: str, **kwargs
) -> str:
    """Helper function to enable passing keyword arguments via a config.

    Args:
        databricks_api (DatabricksAPIClient): The client used to interact with the databricks api.
        github_branch (str): The github branch name.
        kwargs: Key value pair arguments loaded from yaml config.

    Returns:
        str: The final state of the databricks workflow once the run has completed.
    """
    print("kwargs", kwargs["workflow_path"])
    workflow = load_json(kwargs["workflow_path"])
    workflow_name = configure_workflow_name(
        github_branch, kwargs["environment"], workflow
    )
    return run_databricks_workflow(databricks_api, workflow_name=workflow_name)


def run_databricks_workflow(
    databricks_api: DatabricksAPIClient, workflow_name: str
) -> str:
    """Main function that handles the running of a databricks workflow.

    Args:
        databricks_api (DatabricksAPIClient): The client used to interact with the databricks api.
        workflow_name (str): The name of the workflow to run.

    Returns:
        str: The final state of the databricks workflow once the run has completed.
    """
    result_state = "Fail"
    for attempt in range(max_tries := 5):
        try:
            active_run_id = databricks_api.get_active_run_id_by_name(
                workflow_name
            )
            if active_run_id is None:
                print(f"Triggering workflow {workflow_name}")
                run_job_res = databricks_api.trigger_new_job_run(
                    workflow_name
                ).json()
                active_run_id = run_job_res["run_id"]
            status = databricks_api.get_run_state_from_job_id(active_run_id)
            check_counter = 0
            print(status)
            while status["life_cycle_state"] == "RUNNING":
                time.sleep(60)
                status = databricks_api.get_run_state_from_job_id(
                    active_run_id
                )
                print(status)
                check_counter += 1
                if check_counter == 30:
                    print(
                        "Databricks running for more than 30mins, kill tests."
                    )
                    break

            result_state = status.get("result_state", "Fail")
        except Exception as error_info:
            print("Exception caught during query", error_info)
            print(f"Try {attempt+1}/{max_tries}")
            time.sleep(10)
            continue
        if status["life_cycle_state"] == "TERMINATED":
            break
    if result_state != "SUCCESS":
        raise Exception(f"run_id:{active_run_id}: {status}")
    return result_state


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    required_named = parser.add_argument_group("required named arguments")
    required_named.add_argument("-c", "--config-file", required=True)

    optional_named = parser.add_argument_group("optional named arguments")
    optional_named.add_argument(
        "-g",
        "--github-branch-path",
        nargs="?",
        default="main",
    )
    optional_named.add_argument(
        "-w",
        "--workflow_path",
        nargs="?",
        default=None,
    )
    args = parser.parse_args()
    yaml_path = args.config_file

    config = load_yaml(yaml_path)
    config["workflow_path"] = (
        args.workflow_path if args.workflow_path else config["workflow_path"]
    )
    databricks_api = DatabricksAPIClient(
        config["databricks_url"],
        os.environ.get(config["databricks_token_key"]),
    )
    trigger_workflow(
        databricks_api=databricks_api,
        github_branch=args.github_branch_path,
        **config,
    )
