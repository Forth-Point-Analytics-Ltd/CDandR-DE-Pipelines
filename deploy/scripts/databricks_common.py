"""Databricks Common

This file contains code to help utilise the Databricks API
"""

import requests
from enum import Enum
from typing import Optional
from typing import List, Dict
from urllib.parse import urljoin


class RequestTypes(Enum):
    GET = "GET"
    PUT = "PUT"
    POST = "POST"
    PATCH = "PATCH"


class DatabricksAPIClient:
    """Databricks Client API Class

    Interact with common databricks tasks through the API.
    """

    def __init__(self, url: str, token: str) -> None:
        """Constructor for the DatabricksAPIClient

        Args:
            url (str): Databricks workspace URL.
            token (str): Databricks token for authentication.
        """
        self.url = url
        self.token = token
        self.funcs = {
            RequestTypes.GET: requests.get,
            RequestTypes.PUT: requests.put,
            RequestTypes.POST: requests.post,
            RequestTypes.PATCH: requests.patch,
        }

    def _query(
        self,
        path: str,
        request_type: RequestTypes = RequestTypes.GET,
        json: Optional[dict] = None,
    ) -> requests.Response:
        """Wrapper for querying Databricks API

        Wrap the Databricks API Query config ready to be sent with the
        requests library.

        Args:
            path (str): The Databricks api request path.
            request_type (RequestTypes): The request type. Defaults to GET.
            json (Optional[dict]): The optional json to be sent with the request.

        Returns:
            requests.Response: The response from Databricks.

        Raises:
            Exception: If response is not in set of valid status codes.
        """
        print(urljoin(self.url, path))
        response = self.funcs[request_type](
            urljoin(self.url, path),
            headers={"Authorization": f"Bearer {self.token}"},
            json=json,
        )
        print(self.token)

        if response.status_code in (200, 201, 202):
            print("Successfully completed operation")
        else:
            print(response)
            error = response.json()
            raise Exception(f"invalid response: {error['message']}")

        return response

    def get_cluster_id_from_name(self, cluster_name: str) -> str:
        """Get a Databricks cluster id using the cluster name.

        Args:
            cluster_name (str): The name of the cluster.

        Returns:
            str: The cluster id

        Raises:
            ValueError: If no cluster with given name is found.
        """
        print(f"Finding cluster_id for name {cluster_name}")
        response = self._query("/api/2.0/clusters/list")
        clusters = response.json()
        for cluster in clusters.get("clusters", []):
            if cluster["cluster_name"] == cluster_name:
                cluster_id = cluster["cluster_id"]
                print(
                    f"Found cluster_id for name {cluster_name}: {cluster_id}"
                )
                return cluster_id
        raise ValueError(f"No cluster found with name {cluster_name}")

    def create_workflow(self, workflow_config: dict) -> int:
        """Create a new Databricks workflow from a dictionary.

        Args:
            workflow_config (dict): Workflow in dictionary format.

        Returns:
            int: The workflow job id
        """
        print(f"Creating new workflow {workflow_config['settings']['name']}")

        return (
            self._query(
                "/api/2.1/jobs/create",
                json=workflow_config["settings"],
                request_type=RequestTypes.POST,
            )
            .json()
            .get("job_id")
        )

    def update_workflow(self, job_id: int, workflow_config: dict) -> int:
        """Create a new Databricks workflow from a dictionary.

        Args:
            job_id (int): The id of the job
            workflow_config (dict): Workflow in dictionary format.

        Returns:
            int: The workflow job id
        """
        print(f"Updating workflow {workflow_config['settings']['name']}")

        config = {
            "job_id": job_id,
            "new_settings": workflow_config["settings"],
        }
        return (
            self._query(
                "/api/2.1/jobs/update",
                json=config,
                request_type=RequestTypes.POST,
            )
            .json()
            .get("job_id")
        )

    def install_libraries_on_cluster(
        self, cluster_id: str, libraries: List[Dict[str, any]]
    ) -> None:
        """Install libraries onto databricks cluster.

        Args:
            cluster_id (str): The id of the cluster.
            libraries (List[Dict[str, any]]): The libraries to be installed on the cluster.
        """
        print(f"Installing libraries on cluster {cluster_id}: {libraries}")
        self._query(
            "/api/2.0/libraries/install",
            json={"cluster_id": cluster_id, "libraries": libraries},
            request_type=RequestTypes.POST,
        )

    def _get_jobs(self) -> List[Dict[str, any]]:
        """Get list of jobs and and their workflow configurations from Databricks.

        Returns:
            List[Dict[str, any]]: list of Databricks jobs and their workflow configuration.
        """
        return self._query(
            "/api/2.1/jobs/list", request_type=RequestTypes.GET
        ).json()

    def get_job_id_from_name(self, name: str) -> Optional[int]:
        """Get databricks job id from the name of job housing the workflow.

        Args:
            name (str): The cluster name.

        Returns:
            Optional[str]: Job id for workflow.
        """
        jobs = self._get_jobs()
        if "jobs" not in jobs:
            return None
        else:
            jobs = jobs["jobs"]
        for job in jobs:
            if job["settings"]["name"] == name:
                return job["job_id"]
        return None

    def trigger_new_job_run(self, name: str) -> int:
        """Trigger databricks jobs based on the name of the job.

        Args:
            name (str): The name of the workflow to trigger.

        Returns:
            int: Thw unique run id of the workflow
        """
        job_id = self.get_job_id_from_name(name)
        return self._query(
            path="/api/2.1/jobs/run-now",
            json={"job_id": job_id},
            request_type=RequestTypes.POST,
        )

    def get_single_job_run_from_id(self, id: int) -> str:
        """Get job run details using the run id.

        Args:
            id (int): The id of the job run.

        Returns:
            str: An object containing all details related to the run including state.
        """
        return self._query(
            path="/api/2.1/jobs/runs/get",
            json={"run_id": id},
            request_type=RequestTypes.GET,
        )

    def get_active_job_runs(self) -> str:
        """Get all the active job runs

        Returns:
            str: A JSON string containing all the active job runs and some details associated with the runs.
        """
        return self._query(
            "/api/2.1/jobs/runs/list",
            json={"active_only": True},
            request_type=RequestTypes.GET,
        )

    def get_active_run_id_by_name(self, name: str) -> int:
        """Get an active run id using the name of the job run.

        Args:
            name (str): The nam of the databricks workflow.

        Returns:
            int: The unique active run id of the workflow.
        """
        runs = self.get_active_job_runs().json().get("runs")
        if runs is None:
            return runs
        runs_by_name = list(filter(lambda run: run["run_name"] == name, runs))
        if len(runs_by_name) == 0:
            return None
        return runs_by_name[0].get("run_id")

    def get_run_state_from_job_id(self, id: int) -> str:
        """Get the current state of the run using the run id.

        Args:
            id (int): The run id of the workflow.

        Returns:
            str: The state of the current workflow.
        """
        return self.get_single_job_run_from_id(id).json()["state"]
