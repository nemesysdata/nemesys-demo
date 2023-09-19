#!/bin/bash
kubectl delete namespace nemesys-demo
kubectl apply -n nemesys-demo -f ./secrets/github/dcmasterb.yml 
kubectl apply -n nemesys-demo -f ./namespace.yaml
kubectl apply -n nemesys-demo -f ./nemesys-banco.yaml

sleep 90

NEMESYS_HOST=$(kubectl get secrets/nemesys-demodb-pguser-postgres -n nemesys-demo --template='{{.data.host | base64decode }}')
NEMESYS_DATABASE=$(kubectl get secrets/nemesys-demodb-pguser-postgres -n nemesys-demo --template='{{.data.dbname | base64decode }}')
NEMESYS_USUARIO=$(kubectl get secrets/nemesys-demodb-pguser-postgres -n nemesys-demo --template='{{.data.user | base64decode }}')
NEMESYS_PWD=$(kubectl get secrets/nemesys-demodb-pguser-postgres -n nemesys-demo --template='{{.data.password | base64decode }}')

echo "HOST: $NEMESYS_HOST"
echo "DATABASE: $NEMESYS_DATABASE"
echo "USER: $NEMESYS_USUARIO"
echo "PWD: $NEMESYS_PWD"

kubectl create configmap nemesys-demo-vendas-config -n nemesys-demo --from-literal=nemesys-demo-vendas-config.properties='{
"logger": {
        "disable_existing_loggers": false,
        "formatters": {
            "normal": {
                "datefmt": "%Y-%m-%dT%H:%M:%S%z",
                "format": "%(asctime)s %(name)-16s %(levelname)-8s %(threadName)-20s %(funcName)-24s %(message)s"
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "normal",
                "stream": "ext://sys.stdout"
            }
        },
        "root": {
            "handlers": [
                "console"
            ],
            "level": "DEBUG"
        },
        "version": 1
    },    
    "pathfile_lookups": "/var/app",    
    "postgres": {
        "hostname": "'$NEMESYS_HOST'", 
        "database": "'$NEMESYS_DATABASE'",
        "databaseadmin": "'$NEMESYS_DATABASE'",
        "user": "'$NEMESYS_USUARIO'",
        "password": "'$NEMESYS_PWD'"
    }
}'

kubectl apply -n nemesys-demo -f ./nemesys-job.yaml