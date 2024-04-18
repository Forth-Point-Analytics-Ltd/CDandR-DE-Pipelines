<a id="deploy"></a>

# deploy

The deployments script folder containing scripts
and templates for deploying various functionality to databricks.

<a id="deploy.templates"></a>

# deploy.templates

<a id="deploy.scripts.databricks_mount_container"></a>

# deploy.scripts.databricks_mount_container

<a id="deploy.scripts.databricks_mount_container.mount_storage_account"></a>

#### mount_storage_account

```python
def mount_storage_account(dbutils, storage_account: str, databricks_scope: str,
                          sas_token_key: str, container: str,
                          mount_point: str) -> None
```

Mount azure storage to databricks file storage system

Use in a Databricks notebook to mount an azure storage container to the
databricks file system. Usually mounted on to a mount point prefixed with /mnt/.

DISCLAIMER: Databricks does not recommend using this method anymore. https://docs.databricks.com/en/dbfs/mounts.html

**Arguments**:

- `dbutils` _dbutils_ - The Databricks utilities library.
- `storage_account` _str_ - The azure storage account name.
- `databricks_scope` _str_ - The key vault backed databricks scope name.
- `sas_token_key` _str_ - The sas token key name in found in the azure key vault.
- `container` _str_ - The azure storage container name.
- `mount_point` _str_ - The path where the storage container is mounted to.

**Raises**:

- `Exception` - When mount had failed.

**Examples**:

```python
>>> mount_storage_account(
        dbutils = dbutils
        storage_account = "storage_account",
        databricks_scope = "databricks_scope",
        sas_token_key = "sas_token_key",
        container = "container",
        mount_point = "/mnt/mount_point"
    )
```

<a id="deploy.scripts"></a>

# deploy.scripts

The scripts folder contains code to help deploy functionality to databricks.

<a id="deploy.scripts.deploy_databricks_workflow"></a>

# deploy.scripts.deploy_databricks_workflow

<a id="deploy.scripts.deploy_databricks_workflow.update_workflow"></a>

#### update_workflow

```python
def update_workflow(databricks_api: DatabricksAPIClient, cluster_name: str,
                    workflow_path: str, packages_path: str,
                    job_cluster_path: str) -> int
```

Creates new workflow or updates an existing workflow from a job in Databricks

**Arguments**:

- `databricks_api` _DatabricksAPIClient_ - The DataBricksAPIClient to make requests.
- `cluster_name` _str_ - The name of the cluster.
- `workflow_path` _str_ - The local directory path where your cluster config is located.
- `packages_path` _str_ - The local directory path where cluster packages are located.
- `job_cluster_path` _str_ - The local directory path where the job cluster config is located.

**Returns**:

- `int` - The workflow job id

**Examples**:

```python
>>> update_workflow(
        databricks_api = DatabricksAPIClient(
            url = "https://databricks_workspace_url.com"
            token = "databricks_personal_access_token"
        ),
        cluster_name = "cluster_name",
        workflow_path = "path/pipelines/workflow.json",
        packages_path = "path/packages/requirements.txt",
        job_cluster_path = "path/jobs/job_cluster.json",
    )
76
```

<a id="deploy.scripts.databricks_common"></a>

# deploy.scripts.databricks_common

Databricks Common

This file contains code to help utilise the Databricks API

<a id="deploy.scripts.databricks_common.DatabricksAPIClient"></a>

## DatabricksAPIClient Objects

```python
class DatabricksAPIClient()
```

Databricks Client API Class

Interact with common databricks tasks through the API.

<a id="deploy.scripts.databricks_common.DatabricksAPIClient.__init__"></a>

#### \_\_init\_\_

```python
def __init__(url: str, token: str) -> None
```

Constructor for the DatabricksAPIClient

**Arguments**:

- `url` _str_ - Databricks workspace URL.
- `token` _str_ - Databricks token for authentication.

<a id="deploy.scripts.databricks_common.DatabricksAPIClient.get_cluster_id_from_name"></a>

#### get_cluster_id_from_name

```python
def get_cluster_id_from_name(cluster_name: str) -> str
```

Get a Databricks cluster id using the cluster name.

**Arguments**:

- `cluster_name` _str_ - The name of the cluster.

**Returns**:

- `str` - The cluster id

**Raises**:

- `ValueError` - If no cluster with given name is found.

<a id="deploy.scripts.databricks_common.DatabricksAPIClient.create_workflow"></a>

#### create_workflow

```python
def create_workflow(workflow_config: dict) -> int
```

Create a new Databricks workflow from a dictionary.

**Arguments**:

- `workflow_config` _dict_ - Workflow in dictionary format.

**Returns**:

- `int` - The workflow job id

<a id="deploy.scripts.databricks_common.DatabricksAPIClient.update_workflow"></a>

#### update_workflow

```python
def update_workflow(job_id: int, workflow_config: dict) -> int
```

Create a new Databricks workflow from a dictionary.

**Arguments**:

- `job_id` _int_ - The id of the job
- `workflow_config` _dict_ - Workflow in dictionary format.

**Returns**:

- `int` - The workflow job id

<a id="deploy.scripts.databricks_common.DatabricksAPIClient.install_libraries_on_cluster"></a>

#### install_libraries_on_cluster

```python
def install_libraries_on_cluster(cluster_id: str,
                                 libraries: List[Dict[str, any]]) -> None
```

Install libraries onto databricks cluster.

**Arguments**:

- `cluster_id` _str_ - The id of the cluster.
- `libraries` _List[Dict[str, any]]_ - The libraries to be installed on the cluster.

<a id="deploy.scripts.databricks_common.DatabricksAPIClient.get_job_id_from_name"></a>

#### get_job_id_from_name

```python
def get_job_id_from_name(name: str) -> Optional[int]
```

Get databricks job id from the name of job housing the workflow.

**Arguments**:

- `name` _str_ - The cluster name.

**Returns**:

- `Optional[str]` - Job id for workflow.

<a id="deploy.scripts.deploy_libraries_to_cluster"></a>

# deploy.scripts.deploy_libraries_to_cluster

<a id="deploy.scripts.deploy_libraries_to_cluster.install_libraries_on_cluster"></a>

#### install_libraries_on_cluster

```python
def install_libraries_on_cluster(databricks_api: DatabricksAPIClient,
                                 cluster_name: str,
                                 packages_path: str) -> None
```

Install databricks library from a \*.txt file to a cluster.

**Arguments**:

- `databricks_api` _DatabricksAPIClient_ - The DataBricksAPIClient to make requests.
- `cluster_name` _str_ - The name of the cluster.
- `packages_path` _str_ - The local directory path where cluster packages are located.

**Examples**:

```python
>>> install_libraries_on_cluster(
        databricks_api = DatabricksAPIClient(
            url = "https://databricks_workspace_url.com"
            token = "databricks_personal_access_token"
        ),
        cluster_name = "cluster_name",
        packages_path = "path/packages/requirements.txt",
    )
```
