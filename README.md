# vertex_ai_feature_store

```
pip install google-cloud-aiplatform
pip install snowflake-sqlalchemy
pip install pyarrow
```


# Retrieve Serving
```
curl -X POST \
    -H "Authorization: Bearer $(gcloud auth print-access-token)" \
    -H "Content-Type: application/json; charset=utf-8" \
    -d @request.json \
    "https://us-central1-aiplatform.googleapis.com/v1/projects/ff-kubeflow-dev/locations/us-central1/featurestores/ff_fs/entityTypes/shipment_data:streamingReadFeatureValues"
```