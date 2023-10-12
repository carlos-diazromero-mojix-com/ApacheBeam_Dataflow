python data_loader_template_v2.py \
    --host https://tosca.mojixretail.io \
    --apikey BSLDXD7VPACUPDLF \
    --output_table blue-mojix-ai:blue_lakehouse_bronze.yt1_tosca_events_scheduled  \
    --temp_location gs://blue-dataflow/temp \ #--bucket_schema_path blue-dataflow \
    --schema_path ./schemas/bq_tosca_schema.json \
    --partition_column eventTime \
    --start_date 2023-06-12 \
    --end_date 2023-06-12 #--direct_num_workers 4