from connector.snowflake import SnowflakeDB
# from connector.feature_store import *
import pandas as pd
from google.cloud import aiplatform
import datetime
import numpy as np

db = SnowflakeDB()

result = db.fetchall("""
                    select quote_id, AVG_SPOT_RATE, WEIGHT, PALLETIZED_LINEAR_FEET
                    from dev.public.mv_shipment_data
                    order by quote_id desc
                    limit 10
                     """)
df = pd.DataFrame(result)


# Initialize the Vertex SDK for Python
aiplatform.init(project='ff-kubeflow-dev', location='us-central1')

##########################################################################################
# This is to create a new entity type
# get existing Feature Store
fs = aiplatform.featurestore.Featurestore(featurestore_name='ff_fs')

# Create Entity Type
shipment_data_entity_type = fs.create_entity_type(
       entity_type_id="shipment_data",
       description="shipment data",
       )

# create feature
avg_spot_rate = shipment_data_entity_type.create_feature(
        feature_id="avg_spot_rate",
        value_type="STRING", # BOOL, BOOL_ARRAY, DOUBLE, DOUBLE_ARRAY, INT64, INT64_ARRAY, STRING, STRING_ARRAY, BYTES.
        description="average spot rate for shipment",
    )

weight = shipment_data_entity_type.create_feature(
        feature_id="weight",
        value_type="STRING",
        description="weight for shipment",
    )

palletized_linear_feet = shipment_data_entity_type.create_feature(
        feature_id="palletized_linear_feet",
        value_type="STRING",
        description="palletized_linear_feet for shipment",
    )

##########################################################################################

# get existing Feature Store
fs = aiplatform.featurestore.Featurestore(featurestore_name='ff_fs')

  # Get the entity type from an existing Featurestore
shipment_data_entity_type = fs.get_entity_type(entity_type_id="shipment_data")

FEATURES_IDS = [feature.name for feature in shipment_data_entity_type.list_features()]

feature_time_str = datetime.datetime.now().isoformat(sep=" ", timespec="milliseconds")
feature_time = datetime.datetime.strptime(feature_time_str, "%Y-%m-%d %H:%M:%S.%f")

# FEATURE_TIME = "update_time"
ENTITY_ID_FIELD = "shipment_data_id"

# add entity id field and feature time field. GCP wants it to be in TIMESTAMP
df_float = df.apply(pd.to_numeric, downcast='float')

df_float['avg_spot_rate'] = df_float['avg_spot_rate'].astype(str)
df_float['weight'] = df_float['weight'].astype(str)
df_float['avg_spot_rate'] = df_float['avg_spot_rate'].astype(str)
df_float['palletized_linear_feet'] = df_float['palletized_linear_feet'].astype(str)

df_float['update_time'] = feature_time
df_float['update_time'] = pd.to_datetime(
                            df_float['update_time'],
                            format='%Y-%m-%d',
                            errors='coerce'
                        )


# add shipment_data_id field. GCP wants it to be in STRING
df_float['shipment_data_id'] = range(0, df_float.shape[0])
df_float['shipment_data_id'] = 'shipment_data_id_' + df_float['shipment_data_id'].astype(str)


# feature ids should matching with pandas dataframe column names
shipment_data_entity_type.ingest_from_df(
        feature_ids = FEATURES_IDS,
        feature_time = feature_time,
        df_source = df_float,
        entity_id_field = ENTITY_ID_FIELD,
    )


# # Ingest the features
# fs.ingest_from_df(
#     feature_ids=['quote_id',
#                 'avg_spot_rate',
#                 'weight',
#                 'palletized_linear_feet'
#                 ],
#     feature_time=datetime.now().isoformat(sep=" ", timespec="milliseconds"),
#     df_source=df,
#     entity_id_field=entity_id_field
# )


breakpoint()