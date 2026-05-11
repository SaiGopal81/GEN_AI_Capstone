COPY INTO RAW_SALES_BRONZE (order_id, customer_id, product_category, amount, order_date, status)
FROM @data_stage/sales_data.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);