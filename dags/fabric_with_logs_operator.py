import asyncio
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunItemOperator
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.utils.decorators import apply_defaults
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.providers.microsoft.fabric.hooks.run_item import MSFabricRunJobHook

# ------------------ Trigger ------------------
class FabricJobLogsTrigger(BaseTrigger):
    """
    Airflow trigger to poll Fabric notebook status and fetch logs asynchronously.
    """
    def __init__(self, workspace_id: str, item_id: str, run_id: str, poll_interval: int = 10):
        super().__init__()
        self.workspace_id = workspace_id
        self.item_id = item_id
        self.run_id = run_id
        self.poll_interval = poll_interval
        self.hook = MSFabricRunJobHook()

    def serialize(self):
        return (
            "fabric_with_logs_operator.FabricJobLogsTrigger",
            {
                "workspace_id": self.workspace_id,
                "item_id": self.item_id,
                "run_id": self.run_id,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self):
        """
        Async polling loop to get job status and live logs.
        """
        last_log_index = 0

        while True:
            # Fetch run status
            run_status = self.hook.get_run_status(
                workspace_id=self.workspace_id,
                item_id=self.item_id,
                run_id=self.run_id,
            )
            status = run_status.get("status", "Unknown")

            # Fetch logs
            try:
                logs = self.hook.get_job_logs(
                    workspace_id=self.workspace_id,
                    item_id=self.item_id,
                    run_id=self.run_id
                )
                if logs:
                    new_logs = logs[last_log_index:]
                    for line in new_logs:
                        yield TriggerEvent({"log": line})
                    last_log_index = len(logs)
            except Exception as e:
                yield TriggerEvent({"log": f"⚠️ Failed to fetch logs this poll: {e}"})

            if status in ("Completed", "Failed", "Cancelled"):
                yield TriggerEvent({"status": status})
                return

            await asyncio.sleep(self.poll_interval)

# ------------------ Operator ------------------
class MSFabricRunItemWithLiveLogsOperator(MSFabricRunItemOperator):
    """
    Deferrable Fabric operator that prints live notebook logs to Airflow logs.
    """
    @apply_defaults
    def __init__(self, poll_interval: int = 10, **kwargs):
        super().__init__(**kwargs)
        self.poll_interval = poll_interval

    def execute(self, context: Context):
        # Schedule notebook run
        job_instance = super().execute(context)
        if not job_instance:
            raise AirflowException("Fabric did not return a job instance.")

        run_id = job_instance.get("id")
        if not run_id:
            raise AirflowException("Could not extract run_id from Fabric response")

        self.log.info(f"✅ Fabric job submitted. Run ID: {run_id}")

        # Defer the task to async trigger
        self.defer(
            trigger=FabricJobLogsTrigger(
                workspace_id=self.workspace_id,
                item_id=self.item_id,
                run_id=run_id,
                poll_interval=self.poll_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event=None):
        """
        Handle logs/events from trigger.
        """
        if event is None:
            raise AirflowException("No event received from Fabric trigger.")

        if "log" in event:
            self.log.info(event["log"])

        if "status" in event:
            status = event["status"]
            if status != "Completed":
                raise AirflowException(f"Fabric job failed with status: {status}")
            self.log.info("✅ Fabric job completed successfully!")
