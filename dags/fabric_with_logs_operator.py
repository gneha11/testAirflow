# fabric_with_logs_operator.py
import asyncio
from typing import Any
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.context import Context
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunItemOperator
import aiohttp
from airflow.hooks.base import BaseHook
import logging


# ------------------------- TRIGGER -------------------------
class MSFabricRunLogsTrigger(BaseTrigger):
    """
    Async trigger that polls Fabric notebook run and streams logs line by line.
    """
    def __init__(
        self,
        *,
        fabric_conn_id: str,
        workspace_id: str,
        item_id: str,
        run_id: str,
        poll_interval: int = 10,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.fabric_conn_id = fabric_conn_id
        self.workspace_id = workspace_id
        self.item_id = item_id
        self.run_id = run_id
        self.poll_interval = poll_interval

    def serialize(self):
        return (
            "fabric_with_logs_operator.MSFabricRunLogsTrigger",
            {
                "fabric_conn_id": self.fabric_conn_id,
                "workspace_id": self.workspace_id,
                "item_id": self.item_id,
                "run_id": self.run_id,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self):
        """
        Polls the Fabric REST API to stream logs line by line while the job runs.
        """
        # Get connection credentials
        conn = BaseHook.get_connection(self.fabric_conn_id)
        # For OAuth2 SPN connection
        token = conn.password  # Assuming password stores Bearer token or SPN secret
        api_host = "https://api.fabric.microsoft.com"

        logs_seen = set()

        async with aiohttp.ClientSession() as session:
            while True:
                # --- Fetch logs ---
                logs_url = f"{api_host}/v1/workspaces/{self.workspace_id}/items/{self.item_id}/jobs/{self.run_id}/getLog"
                headers = {"Authorization": f"Bearer {token}"}

                async with session.get(logs_url, headers=headers) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    logs = data.get("value", [])

                    for line in logs:
                        if line not in logs_seen:
                            logs_seen.add(line)
                            yield TriggerEvent({"log": line})

                # --- Fetch run status ---
                status_url = f"{api_host}/v1/workspaces/{self.workspace_id}/items/{self.item_id}/jobs/instances/{self.run_id}"
                async with session.get(status_url, headers=headers) as resp:
                    resp.raise_for_status()
                    status_data = await resp.json()
                    status = status_data.get("status")

                    if status in ["Completed", "Failed", "Cancelled"]:
                        yield TriggerEvent({"status": status})
                        return

                await asyncio.sleep(self.poll_interval)


# ------------------------- OPERATOR -------------------------
# ------------------------- OPERATOR -------------------------
class MSFabricRunItemWithLiveLogsOperator(MSFabricRunItemOperator):
    """
    Operator to run a Fabric notebook and stream live logs in Airflow UI.
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
        super().__init__(
            fabric_conn_id=fabric_conn_id,
            workspace_id=workspace_id,
            item_id=item_id,
            job_type=job_type,
            **kwargs
        )

    def execute(self, context: Context):
        # Start the notebook run using the base operator
        run_response = super().execute(context)
        run_id = run_response.get("id")
        if not run_id:
            raise AirflowException("Could not retrieve run_id from Fabric response")

        self.log.info(f"✅ Fabric job submitted. Run ID: {run_id}")

        # Defer execution to async trigger
        self.defer(
            trigger=MSFabricRunLogsTrigger(
                fabric_conn_id=self.fabric_conn_id,
                workspace_id=self.workspace_id,
                item_id=self.item_id,
                run_id=run_id,
                poll_interval=self.poll_interval,
            ),
            method_name="execute_complete"
        )

    def execute_complete(self, context: Context, event: dict[str, Any] = None):
        """
        Called every time the trigger yields a TriggerEvent.
        - If it's a log event → stream it and keep waiting.
        - If it's a status event → finish the task.
        """
        if not event:
            raise AirflowException("No event received from Fabric trigger")

        # Stream logs continuously
        if "log" in event:
            self.log.info(event["log"])
            return None  # keep waiting for more events

        # Final status
        if "status" in event:
            status = event["status"]
            if status == "Completed":
                self.log.info("✅ Fabric job completed successfully!")
                return status
            else:
                raise AirflowException(f"❌ Fabric job ended with status: {status}")

        return event
