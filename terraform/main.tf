resource "databricks_cluster" "etl_cluster" {
  cluster_name            = "etl-free-cluster"
  spark_version           = "13.3.x-scala2.12"
  node_type_id            = "Standard_DS3_v2"
  num_workers             = 1
  autotermination_minutes = 60
}

resource "databricks_notebook" "etl_notebook" {
  path     = "/Shared/etl_pipeline"
  language = "PYTHON"
  source   = "${path.module}/../notebooks/etl_pipeline.py"
}

resource "databricks_job" "etl_job" {
  name = "etl_pipeline_job"

  task {
    task_key = "etl_task"

    new_cluster {
      spark_version = databricks_cluster.etl_cluster.spark_version
      node_type_id  = databricks_cluster.etl_cluster.node_type_id
      num_workers   = 1
    }

    notebook_task {
      notebook_path = databricks_notebook.etl_notebook.path
    }
  }
}
