import asyncio
from typing import Any
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.context import Context
import aiohttp


# ------------------------- ASYNC TRIGGER -------------------------
class MSFabricRunLogsTrigger(BaseTrigger):
    """
    Async trigger to stream Fabric notebook logs and monitor job status.
    """
    def __init__(self, hook, workspace_id: str, item_id: str, run_id: str, poll_interval: int = 10):
        super().__init__()
        self.hook = hook
        self.workspace_id = workspace_id
        self.item_id = item_id
        self.run_id = run_id
        self.poll_interval = poll_interval

    def serialize(self):
        return (
            "fabric_with_live_logs_operator.MSFabricRunLogsTrigger",
            {
                "workspace_id": self.workspace_id,
                "item_id": self.item_id,
                "run_id": self.run_id,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self):
        """
        Poll Fabric job logs and status asynchronously, emitting TriggerEvent for each line.
        """
        async with aiohttp.ClientSession() as session:
            logs_url = f"{self.hook.fabric_base_url}/v1/workspaces/{self.workspace_id}/items/{self.item_id}/jobs/{self.run_id}/getLog"
            status_url = f"{self.hook.fabric_base_url}/v1/workspaces/{self.workspace_id}/items/{self.item_id}/jobs/instances/{self.run_id}"
            headers = self.hook.get_auth_headers()

            while True:
                # Fetch logs
                async with session.get(logs_url, headers=headers) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    logs = data.get("value", [])
                    for line in logs:
                        yield TriggerEvent({"log_line": line})

                # Check job status
                async with session.get(status_url, headers=headers) as status_resp:
                    status_resp.raise_for_status()
                    status_data = await status_resp.json()
                    job_status = status_data.get("status")
                    if job_status in ["Completed", "Failed", "Cancelled"]:
                        yield TriggerEvent({"status": job_status})
                        return

                await asyncio.sleep(self.poll_interval)


# ------------------------- DEFERRED OPERATOR -------------------------
class MSFabricRunItemWithLiveLogsOperator(BaseOperator):
    """
    Operator to submit Fabric notebook and stream live logs line by line.
    """
    def __init__(
        self,
        hook,
        workspace_id: str,
        item_id: str,
        poll_interval: int = 10,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.hook = hook
        self.workspace_id = workspace_id
        self.item_id = item_id
        self.poll_interval = poll_interval

    def execute(self, context: Context):
        # 1️⃣ Submit the notebook/job
        submit_url = f"{self.hook.fabric_base_url}/v1/workspaces/{self.workspace_id}/items/{self.item_id}/jobs/instances"
        headers = self.hook.get_auth_headers()
        import requests
        resp = requests.post(submit_url, headers=headers)
        resp.raise_for_status()
        job_instance = resp.json()

        run_id = job_instance.get("id")
        if not run_id:
            raise AirflowException("Could not retrieve run_id from Fabric response")

        self.log.info(f"✅ Fabric job submitted. Run ID: {run_id}")

        # 2️⃣ Defer task to async trigger for live logs
        self.defer(
            trigger=MSFabricRunLogsTrigger(
                hook=self.hook,
                workspace_id=self.workspace_id,
                item_id=self.item_id,
                run_id=run_id,
                poll_interval=self.poll_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict[str, Any] = None):
        """
        Called by Airflow when trigger emits an event.
        """
        if not event:
            raise AirflowException("No event received from Fabric trigger")

        # Log each line live in UI
        if "log_line" in event:
            self.log.info(event["log_line"])

        # Handle job completion/failure
        if "status" in event:
            status = event["status"]
            if status == "Completed":
                self.log.info("✅ Fabric job completed successfully!")
            else:
                raise AirflowException(f"❌ Fabric job ended with status: {status}")

        return event
