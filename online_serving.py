from google.cloud import aiplatform

aiplatform.init(project='ff-kubeflow-dev', location='us-central1')
  
# get existing Feature Store
fs = aiplatform.featurestore.Featurestore(featurestore_name='ff_fs')

  # Get the entity type from an existing Featurestore
shipment_data_entity_type = fs.get_entity_type(entity_type_id="shipment_data")

FEATURES_IDS = [feature.name for feature in shipment_data_entity_type.list_features()]

my_dataframe = shipment_data_entity_type.read(entity_ids=['shipment_data_id_123'], feature_ids=FEATURES_IDS)