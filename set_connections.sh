#!/bin/bash

# Set Redshift connection
airflow connections add redshift \
    --conn-uri 'redshift://awsuser:R3dsh1ft@default-workgroup.754206996747.us-east-1.redshift-serverless.amazonaws.com:5439/dev'
