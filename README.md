# QueueFlower: Orchestrating Microservice Workflows via Dynamic Queue Balancing

## Setup environment
- A Kubernetes cluster with the microservice system in [DeathstarBench](https://github.com/delimitrou/DeathStarBench)
- Python environment according to [environment.yml](./environment.yml)
- To enable and test HPA please follow [the offical documentation](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale-walkthrough/) 
- To monitor the overhead of Jaeger, please install [Prometheus](https://prometheus.io/) on k8s cluster

## Modules in QueueFlower
We implement different modules of POBO in separate Python scripts as follows:

- [Algorithm](./algorithm.py): Main algorithm of QueueFlower. We also implement the baseline HAB in this script.
- [Jaeger Collector](./jaegerCollector.py): We use Jaeger to implement end-to-end distributed tracing. QueueFlower collects tracing data by leveraging Jaeger's exposed RESTful APIs. To change the sampling rate please refer to [DeathstarBench Documentation](https://github.com/delimitrou/DeathStarBench)
- [Resource Manager](./k8sManager.py): We implement the pod number controller by leveraging the Kubernetes Python client.
- [Workload Generator](./wrk2LoadGenerator.py): We use the commonly used HTTP benchmarking tool wrk2~\cite{wrk2} as the workload generator to send four types of requests, i.e., login, search, reservation, and recommendation, to the application. We provide the functions for generating stationary and non-stationary workload. Wrk2 provides APIs for setting different thread numbers, the number of HTTP connections, the number of requests, the duration, and etc. You can change these parameters according to your cluster configuration.
- [Utils](./utils.py): Helper functions for calculating intermediate results through the pipeline of QueueFlower.
- [Experiment](./main.py): Simply run this bash to replicate our experiment. Certain parameters may need adjustment before execution to align with your environment. For example the endpoints of Jaeger and microservice, cpu and memory limit of pod.