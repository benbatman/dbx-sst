
## Deploying

1. Create the model artifact in Unity Catalog
```bash
databricks bundle deploy -t setup --var="experiment_name=<experiment-name>"
```
2. Run the register model job

```bash
```

## Local Dev

Run locally 
```bash 
node --env-file=.env server-dist/index.js
```