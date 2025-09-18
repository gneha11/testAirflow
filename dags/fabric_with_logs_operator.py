from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunItemOperator
from airflow.utils.context import Context
from airflow.exceptions import AirflowException
import requests

class MSFabricRunItemWithLogsOperator(MSFabricRunItemOperator):
    def execute(self, context: Context):
        job_instance = super().execute(context)
        self.log.info(f"✅ Fabric job submitted. Job instance: {job_instance}")

        try:
            job_instance_id = job_instance["id"]
        except Exception:
            raise AirflowException("Could not extract job_instance_id from Fabric response")

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
                for line in logs:
                    self.log.info(line)
                self.log.info("📒 ===== End Fabric Notebook Logs =====")
            else:
                self.log.warning("⚠️ No logs returned from Fabric API")
        except Exception as e:
            self.log.error(f"❌ Failed to fetch Fabric notebook logs: {e}")

        return job_instance
