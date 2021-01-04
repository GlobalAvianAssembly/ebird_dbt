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
- Once uploaded, create new transfer from google console to import data into bigquery

### Set Up Using DBT
- Create views and tables `dbt run`
- Validate data `dbt test`

### DBT Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](http://slack.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
