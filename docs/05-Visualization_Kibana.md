# Visualization with Kibana

Kibana is the manual UI layer for the demo. The project indexes clean warning
fields into Elasticsearch; the dashboard is created visually in Kibana.

Official Elastic references used for this guide:

- Data views: https://www.elastic.co/docs/explore-analyze/find-and-organize/data-views
- Lens visualizations: https://www.elastic.co/docs/explore-analyze/visualize/lens
- Dashboards: https://www.elastic.co/docs/explore-analyze/dashboards/create-dashboard
- Elasticsearch query rules: https://www.elastic.co/docs/explore-analyze/alerting/alerts/rule-type-es-query

## Access

- URL: http://localhost:5601
- Login user: `elastic`
- Demo password: `admine` unless `ELASTICSEARCH_PASSWORD` is set
- Elasticsearch index: `cicd-observability-events`
- Kibana data view: `cicd-observability-events`
- Time field: `@timestamp`

## Demo Flow

1. Start the stack with `docker compose up -d --build`.
2. Trigger several Jenkins builds.
  a. Or schedule them (* * * * *) for 1 build per minute, and let it run 
3. Open Kibana and set the time picker to `Last 2 hour`.
4. Admire the dashboard as it receives data from the stream.
