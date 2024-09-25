import glob
import os
import re
from typing import Dict, Tuple, TypedDict

import numpy as np
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetOut,
    AssetSelection,
    DynamicPartitionsDefinition,
    Output,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    asset,
    define_asset_job,
    multi_asset,
    sensor,
)
from htmdec_formats import CAGDataset

from .utils import calculate_H, export_CSR_laser_data

csr_2_partitions = DynamicPartitionsDefinition(name="csr_2")

csr_2_file = re.compile(r"^[A-Z]{3}\d{2}_CSR_2_Test\d{3}.zip$")
DATA_PATH = os.environ.get("DATA_PATH", "/home/xarth/codes/htmdec/Jacob_workflow/data")


class FormData(TypedDict):
    sample_id: str
    iteration_id: int
    actuator_parameters: Dict[str, float]
    area_coefficients: Dict[str, float]
    frame_stiffness: float


@asset(description="Input parameters from the DMS form")
def form_data():
    # Get data from Form
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


@asset(description="DataFrame with analysis inputs for the CSR_2 tests")
def analysis_inputs(context: AssetExecutionContext, form_data) -> Output[pd.DataFrame]:
    actuator_parameters = np.array(
        list(form_data["actuator_parameters"].values()), dtype=np.float64
    )
    area_coefficients = np.array(
        list(form_data["area_coefficients"].values()), dtype=np.float64
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
    input_df.loc[input_df.index[0], "Frame Stiffness (N/m)"] = form_data[
        "frame_stiffness"
    ]
    context.log.info(f"Input data: {input_df.head()}")
    return Output(input_df, metadata={"preview": input_df.to_markdown()})


@asset(description="Microscope data in .cag format")
def area_cag(context: AssetExecutionContext):
    fname = glob.glob(f"{DATA_PATH}/*_area.cag")[0]
    sample_id = os.path.basename(fname).split("_")[0]
    context.add_output_metadata({"sample_id": sample_id, "iteration_id": sample_id[:3]})
    return fname


@asset(description="Area measurements extracted from the .cag file")
def area_measurement(context: AssetExecutionContext, area_cag) -> Output[pd.DataFrame]:
    dataset = CAGDataset.from_filename(area_cag)
    data = {"Filename": [], "CS": []}
    for filename, values in dataset.measurements.items():
        if "_CSR_I" not in filename:
            continue
        data["Filename"].append(filename)
        data["CS"].append(values["csa"])

    df = pd.DataFrame(data)
    return Output(df, metadata={"preview": df.to_markdown()})


@asset(
    partitions_def=csr_2_partitions,
    description="Contact surface of individual indentations for CSR",
)
def am_csr_2(context: AssetExecutionContext, area_measurement: pd.DataFrame) -> float:
    sample_id, test_num = context.partition_key.split("_CSR_2_Test")
    context.add_output_metadata({"sample_id": sample_id, "iteration_id": sample_id[:3]})
    test_num = int(test_num[:-4])
    df = area_measurement
    return (
        df.loc[df["Filename"] == f"{sample_id}_CSR_I{test_num:02d}", "CS"]
        .astype(float)
        .values[0]
    )


@asset(partitions_def=csr_2_partitions)
def csr_2_files(context: AssetExecutionContext) -> str:
    sample_id = context.partition_key.split("_CSR_2_Test")[0]
    context.add_output_metadata({"sample_id": sample_id, "iteration_id": sample_id[:3]})
    return os.path.join(DATA_PATH, context.partition_key)


@multi_asset(
    name="measured_quantities",
    partitions_def=csr_2_partitions,
    description="Time, Load, Displacement, and SR extracted from CSR_2 data",
    outs={
        "time": AssetOut(metadata={"quantity": "Time (s)"}),
        "load": AssetOut(metadata={"quantity": "Load (N)"}),
        "displacement": AssetOut(metadata={"quantity": "Displacement (mm)"}),
        "strain_rate": AssetOut(metadata={"quantity": "SR (mm/s)"}),
    },
    group_name="measured_quantities",
)
def measured_quantities(
    context: AssetExecutionContext, csr_2_files: str, analysis_inputs: pd.DataFrame
) -> Tuple[
    Output[pd.DataFrame],
    Output[pd.DataFrame],
    Output[pd.DataFrame],
    Output[pd.DataFrame],
]:
    time, load, displacement, SR = export_CSR_laser_data(
        csr_2_files,
        analysis_inputs["Actuator parameters"].values,
        Kf=analysis_inputs.loc[analysis_inputs.index[0], "Frame Stiffness (N/m)"],
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
        Output(
            df["Time (s)"].to_frame(),
            metadata={"preview": df["Time (s)"].to_markdown()},
        ),
        Output(
            df["Load (N)"].to_frame(),
            metadata={"preview": df["Load (N)"].to_markdown()},
        ),
        Output(
            df["Displacement (mm)"].to_frame(),
            metadata={"preview": df["Displacement (mm)"].to_markdown()},
        ),
        Output(
            df["SR (mm/s)"].to_frame(),
            metadata={"preview": df["SR (mm/s)"].to_markdown()},
        ),
    )


@multi_asset(
    name="computed_quantities",
    partitions_def=csr_2_partitions,
    description="Calculate Hardness and Contact Area from CSR_2 data",
    outs={
        "hardness": AssetOut(metadata={"quantity": "Hardness (GPa)"}),
        "area": AssetOut(metadata={"quantity": "Area (nm^2)"}),
        "hc_over_h": AssetOut(metadata={"quantity": "hc/h"}),
    },
    group_name="computed_quantities",
)
def computed_quantities(
    context: AssetExecutionContext,
    analysis_inputs: pd.DataFrame,
    load: pd.DataFrame,
    displacement: pd.DataFrame,
    am_csr_2: float,
) -> Tuple[Output[pd.DataFrame], Output[pd.DataFrame], Output[pd.Series]]:
    area_coefficients = np.squeeze(
        analysis_inputs["Tip area coefficients"].astype(float).values
    )
    H, A, hc_over_h = calculate_H(
        np.squeeze(load.values),
        np.squeeze(displacement.values),
        area_coefficients,
        area_max_depth=am_csr_2,
    )
    df = pd.DataFrame(
        {
            "Hardness (GPa)": H,
            "Area (nm^2)": A,
        }
    )
    return (
        Output(
            df["Hardness (GPa)"].to_frame(),
            metadata={"preview": df["Hardness (GPa)"].to_markdown()},
        ),
        Output(
            df["Area (nm^2)"].to_frame(),
            metadata={"preview": df["Area (nm^2)"].to_markdown()},
        ),
        Output(
            pd.Series([hc_over_h], name="hc/h"),
            metadata={"preview": f"hc/h: {hc_over_h}"},
        ),
    )


csr_2_job = define_asset_job(
    "csr_2_files_job",
    AssetSelection.assets("csr_2_files"),
    partitions_def=csr_2_partitions,
)


@asset()
def csr_2_summary(
    context: AssetExecutionContext,
    hardness: Dict[str, pd.DataFrame],
    load: Dict[str, pd.DataFrame],
    time: Dict[str, pd.DataFrame],
    displacement: Dict[str, pd.DataFrame],
    hc_over_h: Dict[str, pd.Series],
    area: Dict[str, pd.DataFrame],
    strain_rate: Dict[str, pd.DataFrame],
    analysis_inputs: pd.DataFrame,
) -> None:
    analysis_inputs.to_excel("/tmp/foo.xlsx", sheet_name="Analysis Inputs")
    for key in sorted(hardness.keys()):
        test_num = int(key.split("_CSR_2_Test")[1][:-4])
        # The rest compiles the columns of data into a pandas dataframe
        # outputs this into the excel file.
        df_results = pd.concat(
            [
                time[key],
                displacement[key],
                load[key],
                hardness[key],
                area[key],
                strain_rate[key],
            ],
            axis=1,
            sort=False,
        )
        df_results.columns = [
            "TIME",
            "DEPTH",
            "LOAD",
            "HARDNESS",
            "AREA",
            "STRAIN RATE",
        ]
        df_results.loc[-1] = ["s", "nm", "mN", "GPa", "nm^2", "s^-1"]
        df_results.index = df_results.index + 1
        df_results = df_results.sort_index()
        df_results_hc_over_h = pd.DataFrame(
            np.column_stack((hc_over_h[key], 0)), columns=["hc_over_h", ""]
        )
        with pd.ExcelWriter("/tmp/foo.xlsx", engine="openpyxl", mode="a") as writer:
            df_results.to_excel(writer, sheet_name=f"Test {test_num}", index=False)
            df_results_hc_over_h.to_excel(
                writer, sheet_name=f"Test {test_num} hc_over_h", index=False
            )
    return


@sensor(job=csr_2_job)
def csr_2_sensor(context: SensorEvaluationContext):
    new_csr_2_files = [
        filename
        for filename in os.listdir(DATA_PATH)
        if csr_2_file.match(filename)
        and not csr_2_partitions.has_partition_key(
            filename, dynamic_partitions_store=context.instance
        )
    ]

    return SensorResult(
        run_requests=[
            RunRequest(partition_key=filename) for filename in new_csr_2_files
        ],
        dynamic_partitions_requests=[
            csr_2_partitions.build_add_request(new_csr_2_files)
        ],
    )
