#!/bin/bash
kubectl delete namespace nemesys-demo
kubectl delete -n nemesys-demo -f ./stacks/bancos/postgres/postgres-pv.yml

kubectl create namespace nemesys-demo
kubectl apply -n nemesys-demo -f ./secrets/github/dcmasterb.yml 
kubectl apply -n nemesys-demo -f ./envs/postgres-configmap.yml
kubectl apply -n nemesys-demo -f ./stacks/bancos/postgres/postgres-pvclaim.yml
kubectl apply -n nemesys-demo -f ./stacks/bancos/postgres/postgres-pv.yml
kubectl apply -n nemesys-demo -f ./stacks/bancos/postgres/backend-postgres.yml
kubectl create configmap -n nemesys-demo vendas-config --from-file ./configs/vendas-config.properties
kubectl apply -n nemesys-demo -f ./jobs/job.yml
# kubectl apply -n nemesys-demo -f ./ingress/ingress-lb-private-postgres.yml
#kubectl patch svc postgres -n nemesys-demo -p '{"spec": {"type": "LoadBalancer", "externalIPs":["10.0.0.19","10.60.108.213"]}}'