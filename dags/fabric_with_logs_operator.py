# fabric_live_logs_operator.py
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunItemOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.exceptions import AirflowException
import asyncio
import aiohttp
import datetime

# --------------------- Trigger ---------------------
class MSFabricNotebookTrigger(BaseTrigger):
    def __init__(self, hook, workspace_id, item_id, poll_interval=10):
        self.hook = hook
        self.workspace_id = workspace_id
        self.item_id = item_id
        self.poll_interval = poll_interval

    def serialize(self):
        return ("fabric_live_logs_operator.MSFabricNotebookTrigger", {
            "workspace_id": self.workspace_id,
            "item_id": self.item_id,
            "poll_interval": self.poll_interval,
        })

    async def run(self):
        job_instance = await self.hook.run_item_async(self.workspace_id, self.item_id)
        run_id = job_instance["id"]

        last_log_index = 0
        async with aiohttp.ClientSession() as session:
            while True:
                # Fetch logs incrementally
                async with session.get(
                    f"{self.hook.fabric_base_url}/v1/workspaces/{self.workspace_id}/items/{self.item_id}/jobs/{run_id}/getLog",
                    headers=self.hook.get_auth_headers()
                ) as resp:
                    logs = (await resp.json()).get("value", [])
                    new_logs = logs[last_log_index:]
                    for line in new_logs:
                        yield TriggerEvent({"log": line})
                    last_log_index = len(logs)

                # Check status
                status_resp = await self.hook.get_run_status_async(self.workspace_id, self.item_id, run_id)
                status = status_resp.get("status")
                if status in ["Completed", "Failed", "Cancelled"]:
                    yield TriggerEvent({"status": status})
                    return
                await asyncio.sleep(self.poll_interval)

# --------------------- Operator ---------------------
class MSFabricRunItemWithLiveLogsOperator(MSFabricRunItemOperator):
    def execute(self, context: Context):
        if not getattr(self, "deferrable", False):
            # fallback: blocking live logs
            return super().execute(context)

        self.defer(
            timeout=datetime.timedelta(hours=1),
            trigger=MSFabricNotebookTrigger(self.hook, self.workspace_id, self.item_id),
            method_name="execute_complete"
        )

    def execute_complete(self, context: Context, event=None):
        if not event:
            raise AirflowException("No event returned from trigger")
        if "log" in event:
            self.log.info(event["log"])
        if "status" in event:
            if event["status"] == "Completed":
                self.log.info("✅ Fabric job completed successfully!")
            else:
                raise AirflowException(f"❌ Fabric job failed with status: {event['status']}")
