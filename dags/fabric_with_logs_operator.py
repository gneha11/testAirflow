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
class MSFabricRunItemWithLiveLogsOperator(MSFabricRunItemOperator):
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
        from airflow.providers.microsoft.fabric.hooks.run_item import MSFabricRunItemHook

        # Initialize hook
        hook = MSFabricRunItemHook(fabric_conn_id=self.fabric_conn_id)

        # Start the notebook run
        run_response = super().execute(context)
        run_id = run_response.get("id")
        if not run_id:
            raise AirflowException("Could not retrieve run_id from Fabric response")

        self.log.info(f"✅ Fabric job submitted. Run ID: {run_id}")

        # Defer execution to trigger
        self.defer(
            trigger=MSFabricRunLogsTrigger(
                fabric_conn_id=self.fabric_conn_id,
                workspace_id=self.workspace_id,
                item_id=self.item_id,
                run_id=run_id,
                poll_interval=self.poll_interval
            ),
            method_name="execute_complete"
        )

    def execute_complete(self, context: Context, event: dict[str, Any] = None):
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

