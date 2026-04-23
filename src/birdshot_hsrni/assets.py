import os
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple, TypedDict

import dagster as dg
import numpy as np
import pandas as pd
from dagster import (
    AutoMaterializePolicy,
    AssetExecutionContext,
    AssetOut,
    AssetSelection,
    DynamicPartitionsDefinition,
    Output,
    RunRequest,
    SensorEvaluationContext,
    asset,
    define_asset_job,
    multi_asset,
    sensor,
)
from htmdec_formats import CAGDataset

from .resources import GirderConnection
from .utils import calculate_H, export_CSR_laser_data

indentation_partitions = DynamicPartitionsDefinition(name="indentation")

INDENTATION_FILE_RE = re.compile(r"^[A-Z]{3}\d{2}_CSR_2_Test\d{3}\.zip$")

SRC_FOLDER_ID = os.environ.get("GIRDER_SRC_FOLDER_ID", "")
DST_FOLDER_ID = os.environ.get("GIRDER_DST_FOLDER_ID", "")

_ACTIVE_RUN_STATUSES = [
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
]


class FormData(TypedDict):
    sample_id: str
    iteration_id: int
    actuator_parameters: Dict[str, float]
    area_coefficients: Dict[str, float]
    frame_stiffness: float


@asset(
    description="Input parameters from the DMS form",
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def fetch_indenter_calibration():
    return {
        "sample_id": "BAA01",
        "iteration_id": "BAA",
        "actuator_parameters": {
            "spring_coefficient": 448.155,
            "column_mass": 0.0090855,
            "damping_coefficient": 1.44e-1,
            "conversion_factor": 10.545877075523459,
            "": 0.0,
        },
        "area_coefficients": {
            "m1": 23.67,
            "m2": 1555.5,
            "m3": -18160,
            "m4": 67389,
            "m5": -50152,
        },
        "frame_stiffness": 8e6,
    }


@asset(
    description="DataFrame with analysis inputs for the CSR_2 tests",
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def load_instrument_parameters(context: AssetExecutionContext, fetch_indenter_calibration) -> Output[pd.DataFrame]:
    actuator_parameters = np.array(
        list(fetch_indenter_calibration["actuator_parameters"].values()), dtype=np.float64
    )
    area_coefficients = np.array(
        list(fetch_indenter_calibration["area_coefficients"].values()), dtype=np.float64
    )
    test_input_data_array = np.column_stack((actuator_parameters, area_coefficients))
    input_df_index = [
        "Spring Stiffness (N/m)",
        "Mass (kg)",
        "Damping Coefficient (N s/m)",
        "Conversion Factor (units/mN)",
        "",
    ]
    input_df = pd.DataFrame(
        test_input_data_array,
        columns=["Actuator parameters", "Tip area coefficients"],
        index=input_df_index,
    )
    input_df.loc[input_df.index[0], "Frame Stiffness (N/m)"] = fetch_indenter_calibration[
        "frame_stiffness"
    ]
    context.log.info(f"Input data: {input_df.head()}")
    return Output(input_df, metadata={"preview": input_df.to_markdown()})


@asset(
    partitions_def=indentation_partitions,
    description="Contact surface of individual indentations for CSR",
)
def extract_contact_area(context: AssetExecutionContext, girder: GirderConnection) -> float:
    sample_id, test_part = context.partition_key.split("_CSR_2_Test")
    test_num = int(test_part[:-4])

    items = girder.list_folder_items(SRC_FOLDER_ID)
    cag_items = [i for i in items if i["name"].endswith("_area.cag")]
    cag_path = girder.download_item_to_tempfile(cag_items[0]["_id"], suffix=".cag")

    dataset = CAGDataset.from_filename(cag_path)
    for filename, values in dataset.measurements.items():
        if filename == f"{sample_id}_CSR_I{test_num:02d}":
            context.add_output_metadata({"sample_id": sample_id, "csa": values["csa"]})
            return float(values["csa"])

    raise ValueError(f"No CAG measurement found for {sample_id}_CSR_I{test_num:02d}")


@asset(partitions_def=indentation_partitions)
def fetch_raw_data(context: AssetExecutionContext, girder: GirderConnection) -> str:
    filename = context.partition_key
    items = girder.list_folder_items(SRC_FOLDER_ID, name_regex=re.escape(filename))
    fname = girder.download_item_to_tempfile(items[0]["_id"], suffix=".zip")
    sample_id = filename.split("_CSR_2_Test")[0]
    context.add_output_metadata({"sample_id": sample_id, "iteration_id": sample_id[:3]})
    return fname


@multi_asset(
    name="extract_indentation_signals",
    partitions_def=indentation_partitions,
    description="Time, Load, Displacement, and SR extracted from CSR_2 data",
    outs={
        "time": AssetOut(metadata={"quantity": "Time (s)"}),
        "load": AssetOut(metadata={"quantity": "Load (N)"}),
        "displacement": AssetOut(metadata={"quantity": "Displacement (mm)"}),
        "strain_rate": AssetOut(metadata={"quantity": "SR (mm/s)"}),
    },
    group_name="extract_indentation_signals",
)
def extract_indentation_signals(
    context: AssetExecutionContext, fetch_raw_data: str, load_instrument_parameters: pd.DataFrame
) -> Tuple[
    Output[pd.DataFrame],
    Output[pd.DataFrame],
    Output[pd.DataFrame],
    Output[pd.DataFrame],
]:
    time, load, displacement, SR = export_CSR_laser_data(
        fetch_raw_data,
        load_instrument_parameters["Actuator parameters"].values,
        Kf=load_instrument_parameters.loc[load_instrument_parameters.index[0], "Frame Stiffness (N/m)"],
        downsample=True,
        downsample_hop=500,
    )
    df = pd.DataFrame(
        {
            "Time (s)": time,
            "Load (N)": load,
            "Displacement (mm)": displacement,
            "SR (mm/s)": SR,
        }
    )
    return (
        Output(df["Time (s)"].to_frame(), metadata={"preview": df["Time (s)"].to_markdown()}),
        Output(df["Load (N)"].to_frame(), metadata={"preview": df["Load (N)"].to_markdown()}),
        Output(df["Displacement (mm)"].to_frame(), metadata={"preview": df["Displacement (mm)"].to_markdown()}),
        Output(df["SR (mm/s)"].to_frame(), metadata={"preview": df["SR (mm/s)"].to_markdown()}),
    )


@multi_asset(
    name="compute_mechanical_properties",
    partitions_def=indentation_partitions,
    description="Calculate Hardness and Contact Area from CSR_2 data",
    outs={
        "hardness": AssetOut(metadata={"quantity": "Hardness (GPa)"}),
        "area": AssetOut(metadata={"quantity": "Area (nm^2)"}),
        "hc_over_h": AssetOut(metadata={"quantity": "hc/h"}),
    },
    group_name="compute_mechanical_properties",
)
def compute_mechanical_properties(
    context: AssetExecutionContext,
    load_instrument_parameters: pd.DataFrame,
    load: pd.DataFrame,
    displacement: pd.DataFrame,
    extract_contact_area: float,
) -> Tuple[Output[pd.DataFrame], Output[pd.DataFrame], Output[pd.Series]]:
    area_coefficients = np.squeeze(
        load_instrument_parameters["Tip area coefficients"].astype(float).values
    )
    H, A, hc_over_h = calculate_H(
        np.squeeze(load.values),
        np.squeeze(displacement.values),
        area_coefficients,
        area_max_depth=extract_contact_area,
    )
    df = pd.DataFrame({"Hardness (GPa)": H, "Area (nm^2)": A})
    return (
        Output(df["Hardness (GPa)"].to_frame(), metadata={"preview": df["Hardness (GPa)"].to_markdown()}),
        Output(df["Area (nm^2)"].to_frame(), metadata={"preview": df["Area (nm^2)"].to_markdown()}),
        Output(pd.Series([hc_over_h], name="hc/h"), metadata={"preview": f"hc/h: {hc_over_h}"}),
    )


indentation_job = define_asset_job(
    "indentation_job",
    AssetSelection.assets("fetch_raw_data", "extract_contact_area", "export_results")
    | AssetSelection.groups("extract_indentation_signals", "compute_mechanical_properties"),
    partitions_def=indentation_partitions,
)


@asset(partitions_def=indentation_partitions)
def export_results(
    context: AssetExecutionContext,
    hardness: pd.DataFrame,
    load: pd.DataFrame,
    time: pd.DataFrame,
    displacement: pd.DataFrame,
    hc_over_h: pd.Series,
    area: pd.DataFrame,
    strain_rate: pd.DataFrame,
    load_instrument_parameters: pd.DataFrame,
    girder: GirderConnection,
) -> None:
    key = context.partition_key
    test_num = int(key.split("_CSR_2_Test")[1][:-4])
    output_path = f"/tmp/{key[:-4]}.xlsx"

    load_instrument_parameters.to_excel(output_path, sheet_name="Analysis Inputs")

    df_results = pd.concat(
        [time, displacement, load, hardness, area, strain_rate],
        axis=1,
        sort=False,
    )
    df_results.columns = ["TIME", "DEPTH", "LOAD", "HARDNESS", "AREA", "STRAIN RATE"]
    df_results.loc[-1] = ["s", "nm", "mN", "GPa", "nm^2", "s^-1"]
    df_results.index = df_results.index + 1
    df_results = df_results.sort_index()
    df_results_hc_over_h = pd.DataFrame(
        np.column_stack((hc_over_h, 0)), columns=["hc_over_h", ""]
    )
    with pd.ExcelWriter(output_path, engine="openpyxl", mode="a") as writer:
        df_results.to_excel(writer, sheet_name=f"Test {test_num}", index=False)
        df_results_hc_over_h.to_excel(
            writer, sheet_name=f"Test {test_num} hc_over_h", index=False
        )

    filename = key.replace(".zip", ".xlsx")
    girder.upload_file_to_folder(
        DST_FOLDER_ID,
        output_path,
        mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filename,
    )
    context.add_output_metadata({"output_filename": filename})


@sensor(job=indentation_job, minimum_interval_seconds=60)
def indentation_sensor(context: SensorEvaluationContext, girder: GirderConnection):
    last_poll = context.cursor or "1970-01-01T00:00:00.000000+00:00"

    items = girder.list_folder_items(SRC_FOLDER_ID, name_regex=INDENTATION_FILE_RE.pattern)
    new_items = [i for i in items if i["created"] > last_poll]

    if not new_items:
        return None

    existing = context.instance.get_dynamic_partitions(indentation_partitions.name)
    new_partition_keys = []
    run_requests = []

    for item in new_items:
        key = item["name"]
        if key not in existing:
            new_partition_keys.append(key)
            existing.append(key)

        active = context.instance.get_runs(
            filters=dg.RunsFilter(
                job_name="indentation_job",
                statuses=_ACTIVE_RUN_STATUSES,
                tags={"dagster/partition": key},
            )
        )
        if active:
            context.log.debug(
                f"Skipping partition {key!r}: run {active[0].run_id} is already active."
            )
            continue
        run_requests.append(RunRequest(partition_key=key))

    if new_partition_keys:
        context.instance.add_dynamic_partitions(indentation_partitions.name, new_partition_keys)

    context.update_cursor(
        (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    )
    return run_requests
