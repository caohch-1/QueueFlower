import math

def prop_schedule(queues_estimation, total_capacity):
    """
    prop_schedule is a function to schedule pods for nodes according to the estimated queue length on the nodes
    
    :param queues_estimation: a dict that represents {node: {flow: ave_queue_length, ..., }} and 
    :param total_capacity: total number of pods we use
    :return: a dict that represents {node: pod num}

    let's consider a simple example with two flows and four nodes
    flow 1 goes through {node1, node2, node3} and flow 2 goes through {node1, node2, node4}
    queues_estimation = {node1: {f1: 3, f2: 7}, node2: {f1: 2, f2: 3}, node3: {f1: 5}, node4: {f2: 0}}
    total_capacity = 40
    
    prop_schedule computes and returns the dict pod_on_node
    pod_on_node = {node1: 20, node2: 10, node3: 10, node4: 0}     
    """
    total_queue_in_system = 0.0
    queue_per_node = {}
    for node in queues_estimation:
        queue_per_node[node] = sum([queue_per_flow for _, queue_per_flow in queues_estimation[node].items()])
        total_queue_in_system += queue_per_node[node]
    
    pod_on_node = {}
    for node in queues_estimation:
        pod_on_node[node] = total_capacity*queue_per_node[node]/total_queue_in_system if total_queue_in_system > 0 else 0.0
        pod_on_node[node] = int(pod_on_node[node])
    
    if sum(pod_on_node.values()) < total_capacity:
        max_pod = max(list(pod_on_node.values()))
        for node_name in pod_on_node:
            if pod_on_node[node_name] == max_pod:
                pod_on_node[node_name] += total_capacity - sum(pod_on_node.values())
                return pod_on_node
    else:
        return pod_on_node    

import random
def normalize_probabilities(probabilities):
    total = sum(probabilities)
    normalized_probabilities = [prob / total for prob in probabilities]
    return normalized_probabilities

def sample_from_prob_list(elements, blocking):
    if len(elements) != len(blocking):
        raise ValueError("Number of elements must be equal to the number of probabilities.")
    probabilities = normalize_probabilities(blocking)
    sampled_element = random.choices(elements, weights=probabilities, k=1)[0]
    return sampled_element


def prop_schedule_sla(queues_estimation, ave_delay_vio_estimation, weight, total_capacity):
    """
    prop_schedule is a function to schedule pods for nodes according to the estimated queue length on the nodes

    :param queues_estimation: a dict that represents {node: {flow: ave_queue_length, ..., }} and
    :param ave_delay_vio_estimation: a dict that represents {node: {flow: ave_delay_vio, ..., }} and
    :param weight: a list denotes to balance three metrics (queue, ave delay vio, tail delay vio) and
    :param total_capacity: total number of pods we use
    :return: a dict that represents {node: pod num}
    """
    total_queue_in_system, total_ave_delay_vio,= 0.0, 0.0,
    queue_per_node, ave_delay_vio_per_node,= {}, {}
    for node in queues_estimation:
        queue_per_node[node] = sum([queue_per_flow for _, queue_per_flow in queues_estimation[node].items()])
        total_queue_in_system += queue_per_node[node]

        ave_delay_vio_per_node[node] = sum([ave_delay_vio_per_flow for _, ave_delay_vio_per_flow in ave_delay_vio_estimation[node].items()])
        total_ave_delay_vio += ave_delay_vio_per_node[node]


    blocking_per_node, total_blocking = {}, 0.0
    for node in queues_estimation:
        blocking_per_node[node] = weight[0] * queue_per_node[node]/total_queue_in_system if total_queue_in_system > 0 else 0.0
        blocking_per_node[node] += weight[1] * ave_delay_vio_per_node[node]/total_ave_delay_vio if total_ave_delay_vio > 0 else 0.0
        total_blocking += blocking_per_node[node]

    pod_on_node = {}
    left_pods = total_capacity
    for node in queues_estimation:
        pod_on_node[node] = total_capacity*blocking_per_node[node]/total_blocking if total_blocking > 0 else 0.0
        pod_on_node[node] = int(pod_on_node[node])
        left_pods -= pod_on_node[node]

    if total_blocking == 0:
        return pod_on_node

    nodes, blocking = zip(*blocking_per_node.items())
    probs = normalize_probabilities(blocking)
    while left_pods > 0:
        node = sample_from_prob_list(nodes, probs)
        pod_on_node[node] += 1
        left_pods -= 1

    return pod_on_node

def prop_schedule_sla2(queues_estimation, ave_delay_vio_estimation, tail_delay_vio_estimation, weight, total_capacity):
    """
    prop_schedule is a function to schedule pods for nodes according to the estimated queue length on the nodes

    :param queues_estimation: a dict that represents {node: {flow: ave_queue_length, ..., }} and
    :param ave_delay_vio_estimation: a dict that represents {node: {flow: ave_delay_vio, ..., }} and
    :param tail_delay_vio_estimation: a dict that represents {node: {flow: tail_delay_vio, ..., }} and
    :param weight: a list denotes to balance three metrics (queue, ave delay vio, tail delay vio) and
    :param total_capacity: total number of pods we use
    :return: a dict that represents {node: pod num}
    """
    total_queue_in_system, total_ave_delay_vio, total_tail_delay_vio = 0.0, 0.0, 0.0
    queue_per_node, ave_delay_vio_per_node, tail_delay_vio_per_node = {}, {}, {}
    for node in queues_estimation:
        queue_per_node[node] = sum([queue_per_flow for _, queue_per_flow in queues_estimation[node].items()])
        total_queue_in_system += queue_per_node[node]

        ave_delay_vio_per_node[node] = sum([ave_delay_vio_per_flow for _, ave_delay_vio_per_flow in ave_delay_vio_estimation[node].items()])
        total_ave_delay_vio += ave_delay_vio_per_node[node]

        tail_delay_vio_per_node[node] = sum([tail_delay_vio_per_flow for _, tail_delay_vio_per_flow in tail_delay_vio_estimation[node].items()])
        total_tail_delay_vio += tail_delay_vio_per_node[node]

    blocking_per_node, total_blocking = {}, 0.0
    for node in queues_estimation:
        blocking_per_node[node] = weight[0] * queue_per_node[node]/total_queue_in_system if total_queue_in_system > 0 else 0.0
        blocking_per_node[node] += weight[1] * ave_delay_vio_per_node[node]/total_ave_delay_vio if total_ave_delay_vio > 0 else 0.0
        blocking_per_node[node] += weight[2] * tail_delay_vio_per_node[node]/total_tail_delay_vio if total_tail_delay_vio > 0 else 0.0
        total_blocking += blocking_per_node[node]

    pod_on_node = {}
    left_pods = total_capacity
    for node in queues_estimation:
        pod_on_node[node] = total_capacity*blocking_per_node[node]/total_blocking if total_blocking > 0 else 0.0
        pod_on_node[node] = int(pod_on_node[node])
        left_pods -= pod_on_node[node]

    if total_blocking == 0:
        return pod_on_node

    nodes, blocking = zip(*blocking_per_node.items())
    probs = normalize_probabilities(blocking)
    while left_pods > 0:
        node = sample_from_prob_list(nodes, probs)
        pod_on_node[node] += 1
        left_pods -= 1

    return pod_on_node

"""
   HAB algorithm
"""
from k8sManager import K8sManager
from jaegerCollector import JaegerCollector
from time import sleep, time
import datetime
import pandas as pd
def vs_schedule(queues_estimation, service_time, total_capacity, lambda_, collector: JaegerCollector, k8sManager: K8sManager):
  """
    :param service_time: a dict that denotes the processing time for load of each node, can be average time  {"Node1":0.0003, "Node2":0.0002,...}
    :param total_capacity: total number of pods we use
    :param lambda_: an integer that represents the expectation of the total request rate 
  """
  R_low = 0
  R_up = 3000
  threshold = 500
  lambda_base = 2
  phi_lambda_base = 0
  phi_left = lambda_base
  phi_right = lambda_base * 10
  pod_on_node = {}

  while phi_left < phi_right:
    phi_lambda_base = (phi_left + phi_right)/2
    for node_name in queues_estimation:
        pod_on_node[node_name] = phi_lambda_base * lambda_base * service_time[node_name]
    temp_total = sum(pod_on_node.values())
    if temp_total > total_capacity:
        for node_name in queues_estimation:
            pod_on_node[node_name] = math.floor(total_capacity * pod_on_node[node_name] / temp_total)
            # Here we have a pod num. To learn the parameter, we get a latency of the network now, we denote it by R_lambda_base.
            # At the same time, new requests with total rate lambda_ come into the system, this updates the latency information. 
    for deployment_name, pod_num in pod_on_node.items():
            pod_num += 1
            k8sManager.scale_deployment(deployment_name+"-hotel-hotelres", pod_num)

    sleep(10)
    duration = 10 # Look backward
    limit = 200 # Trace number limit
    R_lambda_base = 0
    tasks = ["HTTP GET /hotels", "HTTP GET /recommendations", "HTTP POST /reservation", "HTTP POST /user"]
    for task in tasks:
            collector.clear()
            # Step1. Collect and process data
            end_time = time()
            _ = collector.collect(end_time=end_time, duration=duration, limit=limit, service="frontend", task_type=task)
            avg_lat, _, _ = collector.calculate_average_latency()
            R_lambda_base += avg_lat
    print(datetime.datetime.now(), f"[HAB Algorithm Output] {R_lambda_base}\n", pd.DataFrame(list(pod_on_node.items()), columns=['Deployment', 'number']))

    if R_lambda_base > (R_up + R_low) / 2 + threshold:
        phi_left = phi_lambda_base
    elif R_lambda_base < (R_up + R_low) / 2 - threshold:
        phi_right = phi_lambda_base
      # Pod_on_node is continuously updated when R_lambda_base > (R_up + R_low) / 2 + threshold or R_lambda_base < (R_up + R_low) / 2 - threshold. Otherwise it's fixed.
    else:
        for node_name in pod_on_node:
            if pod_on_node[node_name] < 1:
                pod_on_node[node_name] = 1

        if sum(pod_on_node.values()) > total_capacity:
            for node_name in pod_on_node:
                pod_on_node[node_name] = math.floor(lambda_ * phi_lambda_base * lambda_ * service_time[node_name]/lambda_base)
            temp_total = sum(pod_on_node.values())
            for node_name in pod_on_node:
                pod_on_node[node_name] = math.floor(total_capacity * pod_on_node[node_name]/temp_total)
        return pod_on_node

