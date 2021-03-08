#!/bin/bash

docker tag airbyte-webapp:latest airbyte-webapp:0.12.1-alpha

docker tag airbyte-webapp:0.12.1-alpha codacy/airbyte-webapp:latest
docker tag airbyte-webapp:0.12.1-alpha codacy/airbyte-webapp:0.12.1-alpha

docker push codacy/airbyte-webapp:latest
docker push codacy/airbyte-webapp:0.12.1-alpha
