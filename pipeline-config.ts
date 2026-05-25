# List all registered source types
GET /pipeline?operation=list-source-types

# List all environments for a source type
GET /pipeline?operation=list-environments&sourceType=PAS

# Get run history (limit is optional, defaults apply)
GET /pipeline?operation=get-run-history&sourceType=PAS&envId=pas-prod&limit=20

# Get topology for an environment
GET /pipeline?operation=get-topology&envId=pas-prod

# List DCM .properties files for an environment
GET /pipeline?operation=list-dcm-files&envId=dcm-uat

# List Payment Central config files
GET /pipeline?operation=list-pc-files&envId=pc-prod

# List connectors from DynamoDB
GET /pipeline?operation=list-connectors
