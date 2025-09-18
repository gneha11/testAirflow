from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunItemOperator
from airflow.utils.context import Context
from airflow.exceptions import AirflowException

class MSFabricRunItemWithLogsOperator(MSFabricRunItemOperator):

    def execute(self, context: Context):
        # Submit job & wait for termination
        job_instance = super().execute(context)
        self.log.info(f"Fabric job submitted. Job instance: {job_instance}")

        # Use hook method to fetch logs
        try:
            logs = self.hook.get_job_logs(
                workspace_id=self.workspace_id,
                item_id=self.item_id,
                job_instance_id=job_instance["id"]
            )
            if logs:
                self.log.info("===== Fabric Notebook Logs =====")
                for line in logs:
                    self.log.info(line)
                self.log.info("===== End Fabric Notebook Logs =====")
            else:
                self.log.warning("No logs returned from Fabric API")
        except Exception as e:
            self.log.error(f"Failed to fetch Fabric notebook logs: {e}")

        return job_instance
