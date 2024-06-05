import requests
import json
import numpy as np
from time import time
import pandas as pd
import re
from utils import get_trace_deployment_table
import datetime
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
pd.options.mode.chained_assignment = None



class JaegerCollector:
    def __init__(self, endpoint: str="16686"):
        self.endpoint = f"http://127.0.0.1:{endpoint}/api/traces"
        self.traces = None

    # Collect traces from Jaeger
    def collect(self, end_time, duration, limit, service, task_type):
        request_data = {
            "start": int((end_time - int(duration)) * 1000000),
            "end": int(end_time * 1000000),
            "operation": task_type,
            "limit": limit,
            "service": service,
            "tags": '{"http.status_code":"200"}'
        }

        response = requests.get(self.endpoint, params=request_data)
        self.traces = json.loads(response.content)["data"]

        # task_type = task_type.replace("/", "")
        # with open(f"./data//trace_{task_type}_{end_time}_{duration}.json", "w") as f:
        #     json.dump(self.traces, f)

        return self.traces
    
    def calculate_duration_difference(self, row, grouped_children):
        if row["childProcessId"] == "NoChild":
            return row["parentDuration"]
        else:
            same_parent_duration_sum = grouped_children.get(row["parentId"], 0)
            return row["parentDuration"] - same_parent_duration_sum

    def process_trace_data(self):
        raw_trace = self.traces
        service_id_mapping = (
            pd.json_normalize(raw_trace)
            .filter(regex="serviceName|traceID|tags")
            .rename(
                columns=lambda x: re.sub(
                    r"processes\.(.*)\.serviceName|processes\.(.*)\.tags",
                    lambda match_obj: match_obj.group(1)
                    if match_obj.group(1)
                    else f"{match_obj.group(2)}Pod",
                    x,
                )
            )
            .rename(columns={"traceID": "traceId"})
        )

        service_id_mapping = (
            service_id_mapping.filter(regex=".*Pod")
            .applymap(
                lambda x: [v["value"] for v in x if v["key"] == "hostname"][0]
                if isinstance(x, list)
                else ""
            )
            .combine_first(service_id_mapping)
        )
        spans_data = pd.json_normalize(raw_trace, record_path="spans")[
            [
                "traceID",
                "spanID",
                "operationName",
                "duration",
                "processID",
                "references",
                "startTime",
            ]
        ]

        spans_with_parent = spans_data[~(spans_data["references"].astype(str) == "[]")]
        root_spans = spans_data[(spans_data["references"].astype(str) == "[]")]
        root_spans = root_spans.rename(
            columns={
                "traceID": "traceId",
                "startTime": "traceTime",
                "duration": "traceLatency"
            }
        )[["traceId", "traceTime", "traceLatency"]]
        spans_with_parent.loc[:, "parentId"] = spans_with_parent["references"].map(
            lambda x: x[0]["spanID"]
        )
        temp_parent_spans = spans_data[
            ["traceID", "spanID", "operationName", "duration", "processID"]
        ].rename(
            columns={
                "spanID": "parentId",
                "processID": "parentProcessId",
                "operationName": "parentOperation",
                "duration": "parentDuration",
                "traceID": "traceId",
            }
        )
        temp_children_spans = spans_with_parent[
            [
                "operationName",
                "duration",
                "parentId",
                "traceID",
                "spanID",
                "processID",
                "startTime",
            ]
        ].rename(
            columns={
                "spanID": "childId",
                "processID": "childProcessId",
                "operationName": "childOperation",
                "duration": "childDuration",
                "traceID": "traceId",
            }
        )
        
        merged_df = pd.merge(
            temp_parent_spans, temp_children_spans, on=["parentId", "traceId"], how="left"
        )

        merged_df = merged_df[
            [
                "traceId",
                "childOperation",
                "childDuration",
                "parentOperation",
                "parentDuration",
                "parentId",
                "childId",
                "parentProcessId",
                "childProcessId",
                "startTime",
            ]
        ]

        merged_df = merged_df.merge(service_id_mapping, on="traceId")
        merged_df = merged_df.merge(root_spans, on="traceId")
        merged_df = merged_df.fillna("NoChild")  # Replace NaN with "NoChild"

        
        merged_df = merged_df.assign(
            childMS=merged_df.apply(
                lambda x: x[x["childProcessId"]] if x["childProcessId"] != "NoChild" else "NoChild",
                axis=1,
            ),
            childPod=merged_df.apply(
                lambda x: x[f"{str(x['childProcessId'])}Pod"] if x["childProcessId"] != "NoChild" else "NoChildPod",
                axis=1,
            ),
            parentMS=merged_df.apply(
                lambda x: x[x["parentProcessId"]],
                axis=1,
            ),
            parentPod=merged_df.apply(
                lambda x: x[f"{str(x['parentProcessId'])}Pod"],
                axis=1,
            ),
            endTime=merged_df.apply(
                lambda x: x["startTime"] + x["childDuration"] if x["childProcessId"] != "NoChild" else "NoChild",
                axis=1,
            ),
        )
        
        grouped_children = merged_df[merged_df["childProcessId"] != "NoChild"].groupby("parentId")["childDuration"].sum()
        merged_df["durationDifference"] = merged_df.apply(lambda x: self.calculate_duration_difference(x, grouped_children), axis=1)
        
        merged_df = merged_df[
            [
                "traceId",
                "traceTime",
                "startTime",
                "endTime",
                "parentId",
                "childId",
                "childOperation",
                "parentOperation",
                "childMS",
                "childPod",
                "parentMS",
                "parentPod",
                "parentDuration",
                "childDuration",
                "durationDifference",
            ]
        ]
        return merged_df

    def calculate_average_latency(self):
        trace_durations = []
        for trace in self.traces:
            trace_duration = max([int(span["duration"]) for span in trace["spans"]])
            trace_durations.append(trace_duration)
        if len(trace_durations) > 0:
            trace_durations = np.array(trace_durations)
            trace_durations.sort()
            average_latency = sum(trace_durations) / len(trace_durations) if len(trace_durations) != 0 else 0
            
            filtered_trace_durations = trace_durations[:int(len(trace_durations)*0.9)]
            average_normal_latency = sum(filtered_trace_durations) / len(filtered_trace_durations) if len(filtered_trace_durations) != 0 else 0

            # sla_violation_trace_durations = [x for x in trace_durations if x >= sla]
            # sla_violations_latency = sum(sla_violation_trace_durations) / len(sla_violation_trace_durations) if len(sla_violation_trace_durations) != 0 else 0
            
            tail_trace_durations = trace_durations[int(len(trace_durations)*0.9):]
            tail_latency = sum(tail_trace_durations) / len(tail_trace_durations)

            print(datetime.datetime.now(), f"[Jaeger] {len(trace_durations)} Average latency is {average_latency} ns.")
            print(datetime.datetime.now(), f"[Jaeger] {len(filtered_trace_durations)} Normal<90\% latency is {average_normal_latency} ns.")
            print(datetime.datetime.now(), f"[Jaeger] {len(tail_trace_durations)} Tail>90\% latency is {tail_latency} ns.")
            return average_latency, average_normal_latency, tail_latency
        else:
            print(datetime.datetime.now(), "[Jaeger] No traces found.")
            return 0, 0, 0
    
    def get_all_latency(self):
        trace_durations = []
        for trace in self.traces:
            trace_duration = max([int(span["duration"]) for span in trace["spans"]])
            trace_durations.append(trace_duration)
        return trace_durations
    
    def clear(self):
        self.traces = None




