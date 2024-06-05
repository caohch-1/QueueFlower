import pandas as pd
from k8sManager import K8sManager
import json
from time import sleep
import csv

def get_trace_deployment_table(merged_df):
    unique_rows_df = merged_df.drop_duplicates(subset=["traceId", "parentId"])
    sum_duration_difference_parent = unique_rows_df.groupby(["traceId", "parentMS"])["durationDifference"].sum().reset_index()


    result_list = []
    for trace_id, group in sum_duration_difference_parent.groupby("traceId"):
        trace_dict = {"traceID": trace_id}
        
        for _, row in group.iterrows():
            trace_dict[row["parentMS"]] = row["durationDifference"]
        
        result_list.append(trace_dict)
    return pd.DataFrame(result_list)

def transform_queue_estimation(input_dict: dict):
    output_dict = {}
    for func, nodes in input_dict.items():
        for node, value in nodes.items():
            if node == "frontend-hotel-hotelres":
                continue
            if node not in output_dict:
                output_dict[node] = {}

            if func not in output_dict[node]:
                output_dict[node][func] = 0

            output_dict[node][func] += value
    return output_dict

def init_env(manager: K8sManager, cpu: int=500, mem: int=500):
    for deployment in manager.deployment_list.items:
        if deployment.metadata.name == "consul-hotel-hotelres":
            manager.set_limit(deployment.metadata.name, 1500, 1000)
            manager.scale_deployment(deployment.metadata.name, 1)
            manager.set_restart(deployment.metadata.name)
        elif deployment.metadata.name == "jaeger-hotel-hotelres":
            continue
        else:
            sleep(2)
            manager.set_limit(deployment.metadata.name, cpu, mem)
            manager.set_request(deployment.metadata.name, cpu/5, mem/5)
            manager.scale_deployment(deployment.metadata.name, 1+2)
            manager.set_restart(deployment.metadata.name)

def save_dict_to_json(data: dict, path):
    with open(path, "w") as json_file:
        json.dump(data, json_file, indent=4)

def calculate_ave_latency_vio(ave_latency: pd.DataFrame):
    sla = pd.read_csv("./data/ave_sla.csv", index_col=0)
    ave_latency_numeric = ave_latency.apply(pd.to_numeric, errors="coerce")
    col_sum = ave_latency_numeric.sum()
    for col in ave_latency_numeric.columns:
        for idx in ave_latency_numeric.index:
            ave_latency_numeric.at[idx, col] = col_sum[col] if pd.notna(ave_latency_numeric.at[idx, col]) else ave_latency_numeric.at[idx, col]
    sla_numeric = sla.apply(pd.to_numeric, errors="coerce")
    return ave_latency_numeric-sla_numeric

def calculate_tail_latency_vio(tail_latency: pd.DataFrame):
    sla = pd.read_csv("./data/tail_sla.csv", index_col=0)
    tail_latency_numeric = tail_latency.apply(pd.to_numeric, errors="coerce")
    col_sum = tail_latency_numeric.sum()
    for col in tail_latency_numeric.columns:
        for idx in tail_latency_numeric.index:
            tail_latency_numeric.at[idx, col] = col_sum[col] if pd.notna(tail_latency_numeric.at[idx, col]) else tail_latency_numeric.at[idx, col]
    
    sla_numeric = sla.apply(pd.to_numeric, errors="coerce")

    return tail_latency_numeric-sla_numeric

def calculate_tail(trace_deployment:pd.DataFrame, quant: float = 0.9):
    tail_lower_bound = trace_deployment.iloc[:, 1:].quantile(quant)
    tail_latency = trace_deployment.iloc[:, 1:][trace_deployment.iloc[:, 1:]>tail_lower_bound]
    return tail_latency.mean()

def prepare_dynamic_workload():
    with open("./data/pattern.csv", "r") as f:
        pattern = [("2m", int(int(row[1])/2)) for row in csv.reader(f) if row[1] != "job_name"]
    return pattern

def scale_checkpoint(manager: K8sManager):
    for deployment in manager.deployment_list.items:
        if deployment.metadata.name == "consul-hotel-hotelres":
            manager.scale_deployment(deployment.metadata.name, 1)
        elif deployment.metadata.name == "jaeger-hotel-hotelres":
            continue

        elif deployment.metadata.name == "frontend-hotel-hotelres":
            manager.scale_deployment(deployment.metadata.name, 2)

        elif deployment.metadata.name == "geo-hotel-hotelres ":
            manager.scale_deployment(deployment.metadata.name, 1)
        elif deployment.metadata.name == "mongodb-geo-hotel-hotelres":
            manager.scale_deployment(deployment.metadata.name, 1)

        elif deployment.metadata.name == "profile-hotel-hotelres ":
            manager.scale_deployment(deployment.metadata.name, 1)
        elif deployment.metadata.name == "memcached-profile-1-hotel-hotelres":
            manager.scale_deployment(deployment.metadata.name, 1)
        elif deployment.metadata.name == "mongodb-profile-hotel-hotelres":
            manager.scale_deployment(deployment.metadata.name, 1)
        
        elif deployment.metadata.name == "rate-hotel-hotelres":
            manager.scale_deployment(deployment.metadata.name, 1)
        elif deployment.metadata.name == "memcached-rate-1-hotel-hotelres":
            manager.scale_deployment(deployment.metadata.name, 1)
        elif deployment.metadata.name == "mongodb-rate-hotel-hotelres":
            manager.scale_deployment(deployment.metadata.name, 1)

        elif deployment.metadata.name == "reservation-hotel-hotelres":
            manager.scale_deployment(deployment.metadata.name, 1)
        elif deployment.metadata.name == "memcached-reserve-1-hotel-hotelres":
            manager.scale_deployment(deployment.metadata.name, 1)
        elif deployment.metadata.name == "mongodb-reservation-hotel-hotelres":
            manager.scale_deployment(deployment.metadata.name, 1)

        elif deployment.metadata.name == "search-hotel-hotelres":
            manager.scale_deployment(deployment.metadata.name, 1)

        elif deployment.metadata.name == "recommendation-hotel-hotelres":
            manager.scale_deployment(deployment.metadata.name, 1)
        elif deployment.metadata.name == "mongodb-recommendation-hotel-hotelres":
            manager.scale_deployment(deployment.metadata.name, 1)

        elif deployment.metadata.name == "user-hotel-hotelres":
            manager.scale_deployment(deployment.metadata.name, 1)
        elif deployment.metadata.name == "mongodb-user-hotel-hotelres":
            manager.scale_deployment(deployment.metadata.name, 1)
        
        