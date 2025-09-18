# fabric_live_logs_operator.py

import time
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunItemOperator
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context


class MSFabricRunItemLiveLogsOperator(MSFabricRunItemOperator):
    """
    Operator to run a Fabric item (notebook, pipeline, etc.)
    and stream logs line by line in Airflow UI until completion.
    """

    def execute(self, context: Context):
        # Start the Fabric job
        job_instance = super().execute(context)

        # Get job instance ID
        job_instance_id = job_instance.get("id")
        if not job_instance_id:
            raise AirflowException("Could not get job_instance_id from Fabric response")

        self.log.info(f"✅ Fabric job submitted. Job instance ID: {job_instance_id}")

        # Poll loop for logs and job status
        while True:
            # Fetch logs
            try:
                logs_data = self.hook.get_job_logs(
                    workspace_id=self.workspace_id,
                    item_id=self.item_id,
                    job_instance_id=job_instance_id
                )
            except Exception as e:
                self.log.warning(f"Could not fetch logs: {e}")
                logs_data = []

            # Print logs line by line
            for line in logs_data:
                self.log.info(line)

            # Fetch job status
            try:
                job_status_data = self.hook.get_job_instance(
                    workspace_id=self.workspace_id,
                    item_id=self.item_id,
                    job_instance_id=job_instance_id
                )
                status = job_status_data.get("status")
            except Exception as e:
                self.log.warning(f"Could not fetch job status: {e}")
                status = None

            # Exit loop if job finished
            if status in ["Completed", "Failed", "Cancelled"]:
                if status == "Completed":
                    self.log.info("✅ Fabric job completed successfully!")
                    return job_instance
                else:
                    raise AirflowException(f"❌ Fabric job ended with status: {status}")

            # Wait before next poll
            time.sleep(10)  # adjust poll interval as needed
