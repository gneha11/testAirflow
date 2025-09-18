import asyncio
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunItemOperator
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.context import Context
from typing import Any
import aiohttp


# ------------------------- TRIGGER -------------------------
class MSFabricRunLogsTrigger(BaseTrigger):
    def __init__(self, hook, workspace_id: str, item_id: str, job_instance_id: str, poll_interval: int = 15):
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
        """Async polling loop to fetch notebook logs."""
        async with aiohttp.ClientSession() as session:
            logs_url = f"{self.hook.fabric_base_url}/v1/workspaces/{self.workspace_id}/items/{self.item_id}/jobs/{self.job_instance_id}/getLog"
            headers = self.hook.get_auth_headers()
            while True:
                async with session.get(logs_url, headers=headers) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    logs = data.get("value", [])
                    if logs:
                        for line in logs:
                            yield TriggerEvent({"log": line})
                    # Check job status
                    status_url = f"{self.hook.fabric_base_url}/v1/workspaces/{self.workspace_id}/items/{self.item_id}/jobs/instances/{self.job_instance_id}"
                    async with session.get(status_url, headers=headers) as status_resp:
                        status_resp.raise_for_status()
                        status_data = await status_resp.json()
                        status = status_data.get("status")
                        if status in ["Completed", "Failed", "Cancelled"]:
                            yield TriggerEvent({"status": status, "log": logs})
                            return
                await asyncio.sleep(self.poll_interval)


# ------------------------- DEFERRED OPERATOR -------------------------
class MSFabricRunItemWithLiveLogsOperator(MSFabricRunItemOperator):
    def execute(self, context: Context):
        """Kick off the Fabric job and defer execution to trigger."""
        job_instance = super().execute(context)

        job_instance_id = job_instance.get("id")
        if not job_instance_id:
            raise AirflowException("Could not get job_instance_id from Fabric response")

        self.log.info(f"✅ Fabric job submitted. Job instance ID: {job_instance_id}")

        # Defer the task to the async trigger
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
        """Called when the trigger fires with event data."""
        if not event:
            raise AirflowException("No event received from Fabric trigger")

        if "log" in event:
            for line in event["log"]:
                self.log.info(line)

        if "status" in event:
            status = event["status"]
            if status == "Completed":
                self.log.info("✅ Fabric job completed successfully!")
            else:
                raise AirflowException(f"❌ Fabric job ended with status: {status}")

        return event
