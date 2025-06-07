minikube delete # If you need to restart with more resources
minikube start --cpus=4 --memory=8192 # Adjust based on your machine's capability
eval $(minikube -p minikube docker-env) # If you plan to build local Docker images accessible by Minikube