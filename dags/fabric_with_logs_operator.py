import asyncio
from typing import Any
import aiohttp

from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunItemOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.utils.context import Context


# ------------------------- TRIGGER -------------------------
class MSFabricRunLogsTrigger(BaseTrigger):
    """
    Async trigger to poll Fabric notebook logs and job status.
    Streams logs line by line to Airflow while job is running.
    """
    SUCCESS_STATUSES = ["Completed", "Succeeded", "Success"]
    FAILURE_STATUSES = ["Failed", "Cancelled", "Error"]

    def __init__(self, hook, workspace_id: str, item_id: str, job_instance_id: str, poll_interval: int = 10):
        super().__init__()
        self.hook = hook
        self.workspace_id = workspace_id
        self.item_id = item_id
        self.job_instance_id = job_instance_id
        self.poll_interval = poll_interval

    def serialize(self):
        return (
            "fabric_with_logs_operator.MSFabricRunLogsTrigger",
            {
                "workspace_id": self.workspace_id,
                "item_id": self.item_id,
                "job_instance_id": self.job_instance_id,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self):
        async with aiohttp.ClientSession() as session:
            headers = self.hook.get_auth_headers()
            logs_url = (
                f"{self.hook.fabric_base_url}/v1/workspaces/"
                f"{self.workspace_id}/items/{self.item_id}/jobs/{self.job_instance_id}/getLog"
            )
            status_url = (
                f"{self.hook.fabric_base_url}/v1/workspaces/"
                f"{self.workspace_id}/items/{self.item_id}/jobs/instances/{self.job_instance_id}"
            )

            last_log_index = 0

            while True:
                # 1️⃣ Fetch logs
                async with session.get(logs_url, headers=headers) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    logs = data.get("value", [])

                    # Stream only new logs
                    new_logs = logs[last_log_index:]
                    if new_logs:
                        for line in new_logs:
                            yield TriggerEvent({"log": line})
                        last_log_index = len(logs)

                # 2️⃣ Fetch job status
                async with session.get(status_url, headers=headers) as resp:
                    resp.raise_for_status()
                    status_data = await resp.json()
                    status = status_data.get("status")

                    if status in self.SUCCESS_STATUSES:
                        yield TriggerEvent({"status": "success"})
                        return
                    elif status in self.FAILURE_STATUSES:
                        yield TriggerEvent({"status": "failure"})
                        return

                # 3️⃣ Sleep before next poll
                await asyncio.sleep(self.poll_interval)


# ------------------------- DEFERRED OPERATOR -------------------------
class MSFabricRunItemWithLiveLogsOperator(MSFabricRunItemOperator):
    """
    Deferrable operator to run a Fabric notebook and stream logs line by line.
    """

    def execute(self, context: Context):
        # 1️⃣ Kick off the Fabric job
        job_instance = super().execute(context)
        job_instance_id = job_instance.get("id")
        if not job_instance_id:
            raise AirflowException("Could not get job_instance_id from Fabric response")

        self.log.info(f"✅ Fabric job submitted. Job instance ID: {job_instance_id}")

        # 2️⃣ Defer execution to trigger
        self.defer(
            trigger=MSFabricRunLogsTrigger(
                hook=self.hook,
                workspace_id=self.workspace_id,
                item_id=self.item_id,
                job_instance_id=job_instance_id,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict[str, Any] = None):
        """
        Called when the trigger fires with event data.
        Streams logs line by line to Airflow UI.
        """
        if not event:
            raise AirflowException("No event received from Fabric trigger")

        # Stream any remaining logs
        if "log" in event:
            self.log.info(event["log"])

        # Handle status
        status = event.get("status")
        if status == "success":
            self.log.info("✅ Fabric job completed successfully!")
        elif status == "failure":
            raise AirflowException("❌ Fabric job failed!")
        else:
            raise AirflowException(f"Unknown job status received: {status}")

        return event
