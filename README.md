
## Deploying

1. Create the model artifact in Unity Catalog

**Note:** `experiment-name` must be a valid Databricks workspace folder path

```bash
databricks bundle deploy -t setup --var="experiment_name=<experiment-name>"
```


2. Run the register model job

     a. Find the job id

        ```bash 
        databricks jobs list
        ```

        And pull the job id from it.
    
    b. Run the job and wait for completion

        ```bash
        databricks jobs run-now <job-id>
        ```

3. Depoy rest of app

Ensure model version matches what is in `databricks.yml`

```bash
databricks bundle deploy -t serving
```

## Local Dev

0. Populate `.env` file in `./app/` dir

```bash
touch ./app/.env
cp .env.example ./app/.env
```

1. Build static files

```bash
npm run build
```

2. Run locally 

```bash 
node --env-file=.env server-dist/index.js
```

3. Go to `http://localhost:8080`

**Note:** If serving endpoint has been inactive, it will have scaled to zero and thus will have a long cold start. Be patient.