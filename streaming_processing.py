from google.cloud import aiplatform


aiplatform.init(project='ff-kubeflow-dev', location='us-central1')

shipment_data_entity_type = aiplatform.featurestore.EntityType(entity_type_name='shipment_data', featurestore_id='ff_fs')

my_data = {
        "shipment_data_id_123": {
            "avg_spot_rate": "12345",
            "weight": "123",
            "palletized_linear_feet": "1111",
        },
    }

shipment_data_entity_type.write_feature_values(instances=my_data)