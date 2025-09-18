# fabric_with_logs_operator.py
import asyncio
from typing import Any
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.context import Context
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunItemOperator
import aiohttp


# ------------------------- TRIGGER -------------------------
class MSFabricRunLogsTrigger(BaseTrigger):
    """
    Async trigger that polls Fabric notebook run and streams logs.
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
            "fabric_with_logs_operator.MSFabricRunLogsTrigger",
            {
                "workspace_id": self.workspace_id,
                "item_id": self.item_id,
                "run_id": self.run_id,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self):
        """Async polling loop to fetch notebook logs and final status."""
        async with aiohttp.ClientSession() as session:
            logs_seen = set()
            while True:
                # --- Fetch logs ---
                logs_url = f"{self.hook.fabric_base_url}/v1/workspaces/{self.workspace_id}/items/{self.item_id}/jobs/{self.run_id}/getLog"
                headers = self.hook.get_auth_headers()
                async with session.get(logs_url, headers=headers) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    logs = data.get("value", [])

                    for line in logs:
                        if line not in logs_seen:
                            logs_seen.add(line)
                            yield TriggerEvent({"log": line})

                # --- Fetch run status ---
                status_url = f"{self.hook.fabric_base_url}/v1/workspaces/{self.workspace_id}/items/{self.item_id}/jobs/instances/{self.run_id}"
                async with session.get(status_url, headers=headers) as resp:
                    resp.raise_for_status()
                    status_data = await resp.json()
                    status = status_data.get("status")

                    if status in ["Completed", "Failed", "Cancelled"]:
                        yield TriggerEvent({"status": status})
                        return

                await asyncio.sleep(self.poll_interval)


# ------------------------- DEFERRED OPERATOR -------------------------
class MSFabricRunItemWithLiveLogsOperator(MSFabricRunItemOperator):
    """
    Operator to run Fabric notebook and stream live logs.
    """
    def __init__(
        self,
        *,
        fabric_conn_id: str,
        workspace_id: str,
        item_id: str,
        job_type: str = "RunNotebook",
        poll_interval: int = 10,
        **kwargs,
    ):
        self.fabric_conn_id = fabric_conn_id
        self.workspace_id = workspace_id
        self.item_id = item_id
        self.job_type = job_type
        self.poll_interval = poll_interval
        self.hook = None
        super().__init__(**kwargs)

    def execute(self, context: Context):
        """Kick off the Fabric job and defer execution to the async trigger."""
        # Initialize hook
        from airflow.providers.microsoft.fabric.hooks.run_item import MSFabricRunItemHook
        self.hook = MSFabricRunItemHook(fabric_conn_id=self.fabric_conn_id)

        # Start the notebook run
        run_response = super().execute(context)
        run_id = run_response.get("id")
        if not run_id:
            raise AirflowException("Could not retrieve run_id from Fabric response")

        self.log.info(f"✅ Fabric job submitted. Run ID: {run_id}")

        # Defer execution to async trigger
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
        """Called when the trigger fires with event data."""
        if not event:
            raise AirflowException("No event received from Fabric trigger")

        if "log" in event:
            self.log.info(event["log"])

        if "status" in event:
            status = event["status"]
            if status == "Completed":
                self.log.info("✅ Fabric job completed successfully!")
            else:
                raise AirflowException(f"❌ Fabric job ended with status: {status}")

        return event
