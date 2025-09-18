import time
import requests
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunItemOperator
from airflow.utils.context import Context
from airflow.exceptions import AirflowException

class MSFabricRunItemWithLiveLogsOperator(MSFabricRunItemOperator):
    """
    Extended MSFabricRunItemOperator to fetch live notebook logs
    in non-deferrable (synchronous) mode.
    """

    poll_interval: int = 10  # seconds between log/poll requests

    def execute(self, context: Context):
        self.log.info("Submitting Fabric job...")
        # Submit the job using the parent operator
        super().execute(context)

        # Wait until Fabric job is available
        try:
            run_id = self._get_latest_run_id()
            if not run_id:
                raise AirflowException("Could not retrieve Fabric run ID after submission.")
        except Exception as e:
            raise AirflowException(f"Failed to get run ID: {e}")

        self.log.info(f"Monitoring Fabric job run_id: {run_id} ...")

        # Poll job status and logs
        job_completed = False
        fetched_logs = set()  # track logs already printed

        while not job_completed:
            # 1️⃣ Fetch job status
            run_details = self.hook.get_run(run_id)
            status = run_details.get("status")
            self.log.info(f"Job status: {status}")

            # 2️⃣ Fetch logs
            logs = self._fetch_logs(run_id)
            for line in logs:
                if line not in fetched_logs:
                    self.log.info(line)
                    fetched_logs.add(line)

            if status in ["Completed", "Failed", "Cancelled"]:
                job_completed = True
            else:
                time.sleep(self.poll_interval)

        if status != "Completed":
            raise AirflowException(f"Fabric job finished with status: {status}")

        self.log.info("✅ Fabric job completed successfully!")

    def _get_latest_run_id(self) -> str:
        """
        Fetch the most recent run ID for this workspace/item.
        """
        runs = self.hook.list_runs(workspace_id=self.workspace_id, item_id=self.item_id)
        if not runs:
            return None
        # Return the most recent run (assumes list_runs returns newest first)
        return runs[0]["id"]

    def _fetch_logs(self, run_id: str):
        """
        Fetch logs from Fabric API for a given run_id.
        """
        try:
            log_url = f"{self.hook.fabric_base_url}/v1/workspaces/{self.workspace_id}/items/{self.item_id}/jobs/{run_id}/getLog"
            headers = self.hook.get_auth_headers()
            response = requests.get(log_url, headers=headers)
            response.raise_for_status()
            return response.json().get("value", [])
        except Exception as e:
            self.log.warning(f"Failed to fetch logs: {e}")
            return []
