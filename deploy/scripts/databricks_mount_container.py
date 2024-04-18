def mount_storage_account(
    dbutils,
    storage_account: str,
    databricks_scope: str,
    sas_token_key: str,
    container: str,
    mount_point: str,
) -> None:
    """Mount azure storage to databricks file storage system

    Use in a Databricks notebook to mount an azure storage container to the
    databricks file system. Usually mounted on to a mount point prefixed with /mnt/.

    DISCLAIMER: Databricks does not recommend using this method anymore. https://docs.databricks.com/en/dbfs/mounts.html

    Args:
        dbutils (dbutils): The Databricks utilities library.
        storage_account (str): The azure storage account name.
        databricks_scope (str): The key vault backed databricks scope name.
        sas_token_key (str): The sas token key name in found in the azure key vault.
        container (str): The azure storage container name.
        mount_point (str): The path where the storage container is mounted to.

    Raises:
        Exception: When mount had failed.

    Examples:
        >>> mount_storage_account(
            dbutils = dbutils
            storage_account = "storage_account",
            databricks_scope = "databricks_scope",
            sas_token_key = "sas_token_key",
            container = "container",
            mount_point = "/mnt/mount_point"
        )
    """
    storageAccountName = storage_account
    sasToken = dbutils.secrets.get(scope=databricks_scope, key=sas_token_key)
    blobContainerName = container
    mountPoint = mount_point
    if not any(
        mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()
    ):
        try:
            dbutils.fs.mount(
                source=f"wasbs://{blobContainerName}@{storageAccountName}.blob.core.windows.net",
                mount_point=mountPoint,
                extra_configs={
                    f"fs.azure.sas.{blobContainerName}.{storageAccountName}.blob.core.windows.net": sasToken
                },
            )
            print("mount succeeded!")
        except Exception as e:
            print("mount exception", e)
