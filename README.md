# Data 

### Requirements
- [BigQuery project](https://cloud.google.com/bigquery/)
- [eBird data](https://ebird.org/data/download)
- Install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install)
- Install [DBT](https://docs.getdbt.com/dbt-cli/installation/)
- (optional for ebird data import) [GCS](https://cloud.google.com/storage/)

### Set Up DBT
- Configure DBT under `~/.dbt/profiles.yml`
```yaml
urban_ebird:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: '[google project number]'
      dataset: 'dropbox'
      threads: 1
      timeout_seconds: 300
      location: EU
      priority: interactive
      retries: 1
~                 
```

- Connect bigquery and dbt
```bash
gcloud auth application-default login \
  --scopes=https://www.googleapis.com/auth/userinfo.email,\
https://www.googleapis.com/auth/cloud-platform,\
https://www.googleapis.com/auth/drive.readonly
```

### Data import
- Data included in this project can be imported using `dbt seed`
- eBird data needs importing separately, this can be done by uploading the ebird data file into GCS and then transferring into a big query table.
- If using SDK, set project name `gcloud config set project [google project ID]`
- Set up GCS and create a new bucket for the ebird upload. Using SDK: `gsutil mb -b on -l eu gs://ebird/`
- Upload ebird into new bucket. Using SDK `gsutil cp [path to ebird download] gs://ebird/`
- Create target table in bigquery for ebird data
```sql
create table dropbox.ebird (
global_unique_identifier STRING,
last_edited_date DATETIME,
taxonomic_order INT64,
category STRING,
common_name STRING,
scientific_name STRING,
subspecies_common_name 	STRING,
subspecies_scientific_name 	STRING,
observation_count STRING,
breeding_bird_atlas_code STRING,
breeding_bird_atlas_category STRING,
age_sex STRING,
country STRING,
country_code STRING,
state 	STRING,
state_code 	STRING,
county 	STRING, 		
county_code 	STRING,
iba_code STRING,
bcr_code STRING,
usfws_code STRING,
atlas_block STRING,
locality 	STRING,
locality_id 	STRING,
locality_type 	STRING,
latitude 	FLOAT64,
longitude 	FLOAT64,
observation_date 	DATE,
time_observations_started 	STRING,
observer_id 	STRING,
sampling_event_identifier 	STRING,
protocol_type 	STRING,
protocol_code 	STRING, 	
project_code 	STRING,
duration_minutes INT64,
effort_distance_km 	FLOAT64,
effort_area_ha 	FLOAT64,
number_observers 	INT64,
all_species_reported 	INT64,
group_identifier 	STRING,
has_media 	INT64,
approved 	INT64,
reviewed 	INT64,
reason 	STRING,
trip_comments 	STRING ,
species_comments 	STRING 	
);
```
- Create new transfer from [google console](https://console.cloud.google.com/bigquery/transfers) to import data into bigquery.  Select file in google cloud storage  as source, and new table in bigquery as target. File format should be `CSV`, field separator should be `\t` and skip header rows should be `1`.

### Set Up Using DBT
- Create views and tables `dbt run`
- Validate data `dbt test`

### Structure
This will create four different datasets in bigquery.
- `dropbox`, used for seed and eBird data
- `ebird`, used for views defining eBird and city structures
- `intermediate`, used for tables to reduce data queried in models
- `model`, holds the final data sets for modelling

### Landscape and habitat variables
These were generated with google earth engine.
The code can be found here:

https://code.earthengine.google.com/?accept_repo=users/jamesr/city_metrics

### DBT Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](http://slack.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
