from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunItemOperator
from airflow.utils.context import Context
from airflow.exceptions import AirflowException
import requests


class MSFabricRunItemWithLogsOperator(MSFabricRunItemOperator):
    """
    Extends MSFabricRunItemOperator to fetch and stream Fabric notebook logs into Airflow logs.
    """

    def execute(self, context: Context):
        job_instance = super().execute(context)
        self.log.info(f"✅ Fabric job submitted. Raw response: {job_instance}")

        # --- Extract job instance ID ---
        job_instance_id = (
            job_instance.get("id")
            or job_instance.get("run_id")
            or job_instance.get("tracker", {}).get("item", {}).get("run_id")
        )
        if not job_instance_id:
            raise AirflowException(
                f"❌ Could not extract job_instance_id from Fabric response: {job_instance}"
            )

        # --- Fetch logs ---
        try:
            log_url = (
                f"{self.hook.fabric_base_url}/v1/workspaces/{self.workspace_id}/"
                f"items/{self.item_id}/jobs/{job_instance_id}/getLog"
            )
            headers = self.hook.get_auth_headers()
            response = requests.get(log_url, headers=headers)
            response.raise_for_status()

            logs = response.json().get("value", [])
            if logs:
                self.log.info("📒 ===== Fabric Notebook Logs =====")
                for entry in logs:
                    if isinstance(entry, dict) and "message" in entry:
                        self.log.info(entry["message"])
                    else:
                        self.log.info(str(entry))
                self.log.info("📒 ===== End Fabric Notebook Logs =====")
            else:
                self.log.warning("⚠️ No logs returned from Fabric API")
        except Exception as e:
            self.log.error(f"❌ Failed to fetch Fabric notebook logs: {e}")

        return job_instance
