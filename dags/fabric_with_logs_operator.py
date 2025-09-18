from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunItemOperator
from airflow.utils.context import Context
from airflow.exceptions import AirflowException
import time

class MSFabricRunItemWithLiveLogsOperator(MSFabricRunItemOperator):
    """
    Extended Fabric operator to fetch live logs during execution.
    Works only when deferrable=True.
    """

    def execute(self, context: Context):
        # Schedule the notebook run (inherited)
        job_instance = super().execute(context)
        
        if not job_instance:
            raise AirflowException("Fabric did not return a job instance.")

        run_id = job_instance.get("id")
        if not run_id:
            raise AirflowException("Could not extract run_id from Fabric response")

        self.log.info(f"✅ Fabric job submitted. Run ID: {run_id}")

        # Poll the job status and fetch logs
        status = None
        while status not in ("Completed", "Failed", "Cancelled"):
            # Fetch run details
            run_details = self.hook.get_run_status(
                workspace_id=self.workspace_id,
                item_id=self.item_id,
                run_id=run_id,
            )
            status = run_details.get("status")
            self.log.info(f"⏳ Job status: {status}")

            # Fetch new logs
            try:
                logs = self.hook.get_job_logs(
                    workspace_id=self.workspace_id,
                    item_id=self.item_id,
                    run_id=run_id
                )
                if logs:
                    for line in logs:
                        self.log.info(f"📒 {line}")
            except Exception as e:
                self.log.warning(f"Could not fetch logs this poll: {e}")

            if status not in ("Completed", "Failed", "Cancelled"):
                # Wait some seconds before next poll
                time.sleep(10)  # adjust polling interval as needed

        if status != "Completed":
            raise AirflowException(f"Fabric job did not complete successfully. Status: {status}")

        self.log.info("✅ Fabric job completed successfully!")
        return job_instance
