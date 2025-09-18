from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunItemOperator
from airflow.utils.context import Context
from airflow.exceptions import AirflowException
import time
import requests

class MSFabricRunItemWithLiveLogsOperator(MSFabricRunItemOperator):
    def execute(self, context: Context):
        # Submit the job
        job_instance = super().execute(context)
        self.log.info(f"✅ Fabric job submitted. Job instance: {job_instance}")

        try:
            job_instance_id = job_instance["id"]
        except Exception:
            raise AirflowException("Could not extract job_instance_id from Fabric response")

        headers = self.hook.get_auth_headers()
        last_line_index = 0
        poll_interval = 10  # seconds
        max_retries = 360  # ~1 hour

        for _ in range(max_retries):
            # Step 1: Check job status
            run_url = (
                f"{self.hook.fabric_base_url}/v1/workspaces/{self.workspace_id}/"
                f"items/{self.item_id}/jobs/instances/{job_instance_id}"
            )
            resp = requests.get(run_url, headers=headers)
            resp.raise_for_status()
            run_info = resp.json()
            status = run_info.get("status")

            # Step 2: Fetch logs incrementally
            log_url = (
                f"{self.hook.fabric_base_url}/v1/workspaces/{self.workspace_id}/"
                f"items/{self.item_id}/jobs/{job_instance_id}/getLog"
            )
            log_resp = requests.get(log_url, headers=headers)
            log_resp.raise_for_status()
            logs = log_resp.json().get("value", [])

            # Print only new lines
            for line in logs[last_line_index:]:
                self.log.info(line)
            last_line_index = len(logs)

            if status in ("Completed", "Failed", "Cancelled"):
                break

            time.sleep(poll_interval)

        self.log.info(f"📌 Final Fabric job status: {status}")

        if status != "Completed":
            raise AirflowException(f"Fabric job did not complete successfully (status={status})")

        return job_instance
