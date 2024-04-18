import argparse
from databricks_common import DatabricksAPIClient
from dotenv import load_dotenv
import os

load_dotenv()


def install_libraries_on_cluster(
    databricks_api: DatabricksAPIClient, cluster_name: str, packages_path: str
) -> None:
    """Install databricks library from a *.txt file to a cluster.

    Args:
        databricks_api (DatabricksAPIClient): The DataBricksAPIClient to make requests.
        cluster_name (str): The name of the cluster.
        packages_path (str): The local directory path where cluster packages are located.

    Examples:
        >>> install_libraries_on_cluster(
            databricks_api =  DatabricksAPIClient(
                url = "https://databricks_workspace_url.com"
                token = "databricks_personal_access_token"
            ),
            cluster_name = "cluster_name",
            packages_path = "path/packages/requirements.txt",
        )
    """
    cluster_id = databricks_api.get_cluster_id_from_name(cluster_name)
    with open(packages_path) as packages:
        libraries = []
        for package in packages.readlines():
            pypi_package = {"pypi": {"package": package}}
            libraries.append(pypi_package)
        databricks_api.install_libraries_on_cluster(cluster_id, libraries)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    required_named = parser.add_argument_group("required named arguments")
    required_named.add_argument("-u", "--databricks-url", required=True)
    required_named.add_argument("-c", "--cluster-name", required=True)
    optional_named = parser.add_argument_group("optional named arguments")
    optional_named.add_argument(
        "-r",
        "--requirements-path",
        nargs="?",
        default="requirements.txt",
    )
    args = parser.parse_args()
    databricks_api = DatabricksAPIClient(
        args.databricks_url, os.environ["API_TOKEN"]
    )

    install_libraries_on_cluster(
        databricks_api, args.cluster_name, args.requirements_path
    )
