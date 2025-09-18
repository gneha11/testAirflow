from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunItemOperator
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.context import Context
import asyncio
import aiohttp

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
        # Save custom args first
        self.fabric_conn_id = fabric_conn_id
        self.workspace_id = workspace_id
        self.item_id = item_id
        self.job_type = job_type
        self.poll_interval = poll_interval
        self.hook = None

        # Only pass what the parent operator expects to super().__init__
        # MSFabricRunItemOperator usually expects: task_id, dag, deferrable, wait_for_termination, etc.
        # Do NOT pass your custom args to super().__init__
        super().__init__(**kwargs)


    def execute(self, context: Context):
        from airflow.providers.microsoft.fabric.hooks.run_item import MSFabricRunItemHook  # import inside execute
        if not self.hook:
            self.hook = MSFabricRunItemHook(fabric_conn_id=self.fabric_conn_id)

        job_instance = super().execute(context)

        job_instance_id = job_instance.get("id")
        if not job_instance_id:
            raise AirflowException("Could not get job_instance_id from Fabric response")

        self.log.info(f"✅ Fabric job submitted. Job instance ID: {job_instance_id}")

        self.defer(
            trigger=MSFabricRunLogsTrigger(
                hook=self.hook,
                workspace_id=self.workspace_id,
                item_id=self.item_id,
                job_instance_id=job_instance_id,
                poll_interval=self.poll_interval
            ),
            method_name="execute_complete"
        )

    def execute_complete(self, context: Context, event: dict = None):
        if not event:
            raise AirflowException("No event received from Fabric trigger")
        if "log" in event:
            for line in event["log"]:
                self.log.info(line)
        if "status" in event:
            status = event["status"]
            if status != "Completed":
                raise AirflowException(f"❌ Fabric job ended with status: {status}")
            self.log.info("✅ Fabric job completed successfully!")
        return event
