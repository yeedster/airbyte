FROM airbyte/webapp:0.12.1-alpha

EXPOSE 80

COPY nginx/default.conf.template /etc/nginx/templates/default.conf.template
