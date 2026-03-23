# Japan Weather ETL

This repository creates a data pipeline in databricks for Japan weather and events centered around train stations.

### Deployment steps
1. Databricks DABs are used to deploy the pipeline, a YAML file is created to deploy this pipeline [dab](databricks.yaml)
2. Run the SQL commands to create catalog, schema, volumes [create-env.sql](create-env.sql)
3. Deployment steps as follows:
   ```bash
   databricks bundle validate
   ```
   ```bash
   databricks bundle deploy
   ```