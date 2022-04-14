FROM nginx:1.20.2-alpine as webapp

EXPOSE 80

COPY build /usr/share/nginx/html
COPY nginx/default.conf.template /etc/nginx/templates/default.conf.template
