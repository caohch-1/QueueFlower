from kubernetes import client, config
from time import sleep
import datetime


class K8sManager:
    def __init__(self, namespace):
        config.load_kube_config()

        self.api_client_corev1 = client.CoreV1Api()
        self.api_client_appsv1 = client.AppsV1Api()
        self.namespace = namespace

        self.pod_list = self.api_client_corev1.list_namespaced_pod(
            namespace=self.namespace)
        self.service_list = self.api_client_corev1.list_namespaced_service(
            namespace=self.namespace)
        self.deployment_list = self.api_client_appsv1.list_namespaced_deployment(
            namespace=self.namespace)

    def update(self):
        self.pod_list = self.api_client_corev1.list_namespaced_pod(
            namespace=self.namespace)
        self.service_list = self.api_client_corev1.list_namespaced_service(
            namespace=self.namespace)
        self.deployment_list = self.api_client_appsv1.list_namespaced_deployment(
            namespace=self.namespace)

    def get_pods_name_list(self):
        return [pod.metadata.name for pod in self.pod_list.items]

    def scale_deployment(self, deployment_name, replica_num):
        deployment = self.api_client_appsv1.read_namespaced_deployment(
            deployment_name, self.namespace)
        if deployment.spec.replicas != replica_num:
            deployment.spec.replicas = replica_num
            self.api_client_appsv1.patch_namespaced_deployment_scale(
                    name=deployment_name, namespace=self.namespace, body=deployment)
            while True:
                deployment = self.api_client_appsv1.read_namespaced_deployment(
                    deployment_name, self.namespace)
                if (
                    deployment.status.available_replicas == deployment.spec.replicas
                    and deployment.status.replicas == deployment.spec.replicas
                    and deployment.status.updated_replicas == deployment.spec.replicas
                ):
                    break
            print(datetime.datetime.now(),
                f"[K8sManager Scale] Scale {deployment_name} to {replica_num} pods.")
            sleep(2.5)
        else:
            print(datetime.datetime.now(),
                f"[K8sManager Scale] Keep {deployment_name} have {replica_num} pods.")

    def set_limit(self, deployment_name, cpu_limit, mem_limit):
        deployment = self.api_client_appsv1.read_namespaced_deployment(
            deployment_name, self.namespace)
        if deployment.spec.template.spec.containers[0].resources.limits:
            old_cpu_limit = deployment.spec.template.spec.containers[0].resources.limits["cpu"]
            deployment.spec.template.spec.containers[0].resources.limits["cpu"] = f"{cpu_limit}m"
            old_memory_limit = deployment.spec.template.spec.containers[0].resources.limits["memory"]
            deployment.spec.template.spec.containers[0].resources.limits["memory"] = f"{mem_limit}Mi"
            self.api_client_appsv1.patch_namespaced_deployment(
                name=deployment_name, namespace=self.namespace, body=deployment)
            print(datetime.datetime.now(),
                f"[K8sManager Limit] Set {deployment_name} limit cpu from {old_cpu_limit} to {cpu_limit}m.")
            print(datetime.datetime.now(),
                f"[K8sManager Limit] Set {deployment_name} limit memory from {old_memory_limit} to {mem_limit}Mi.")
        else:
            deployment.spec.template.spec.containers[0].resources.limits = {"cpu": f"{cpu_limit}m", "memory": f"{mem_limit}Mi"}
            self.api_client_appsv1.patch_namespaced_deployment(
                name=deployment_name, namespace=self.namespace, body=deployment)
            print(datetime.datetime.now(),
                f"[K8sManager Limit] Set {deployment_name} limit cpu from Unlimited to {cpu_limit}m.")
            print(datetime.datetime.now(),
                f"[K8sManager Limit] Set {deployment_name} limit memory from Unlimited to {mem_limit}Mi.")
        sleep(2)

    def set_request(self, deployment_name, cpu_request, mem_request):
        deployment = self.api_client_appsv1.read_namespaced_deployment(
            deployment_name, self.namespace)
        if deployment.spec.template.spec.containers[0].resources.requests:
            old_cpu_request = deployment.spec.template.spec.containers[0].resources.requests["cpu"]
            deployment.spec.template.spec.containers[0].resources.requests["cpu"] = f"{cpu_request}m"
            old_memory_request = deployment.spec.template.spec.containers[0].resources.requests["memory"]
            deployment.spec.template.spec.containers[0].resources.requests["memory"] = f"{mem_request}Mi"
            self.api_client_appsv1.patch_namespaced_deployment(
                name=deployment_name, namespace=self.namespace, body=deployment)
            print(datetime.datetime.now(),
                f"[K8sManager request] Set {deployment_name} request cpu from {old_cpu_request} to {cpu_request}m.")
            print(datetime.datetime.now(),
                f"[K8sManager request] Set {deployment_name} request memory from {old_memory_request} to {mem_request}Mi.")
        else:
            deployment.spec.template.spec.containers[0].resources.requests = {"cpu": f"{cpu_request}m", "memory": f"{mem_request}Mi"}
            self.api_client_appsv1.patch_namespaced_deployment(
                name=deployment_name, namespace=self.namespace, body=deployment)
            print(datetime.datetime.now(),
                f"[K8sManager Request] Set {deployment_name} request cpu from Unrequested to {cpu_request}m.")
            print(datetime.datetime.now(),
                f"[K8sManager Request] Set {deployment_name} request memory from Unrequested to {mem_request}Mi.")
        sleep(2)

    def set_restart(self, deployment_name):
        deployment = self.api_client_appsv1.read_namespaced_deployment(
            deployment_name, self.namespace)
        deployment.spec.template.spec.restart_policy = "Always"
        self.api_client_appsv1.patch_namespaced_deployment(
                name=deployment_name, namespace=self.namespace, body=deployment)
        print(datetime.datetime.now(),
                f"[K8sManager Restart Policy] Set {deployment_name} restart policy to True.")
        sleep(2)