from dagster import Definitions, EnvVar, load_assets_from_modules

from . import assets
from .resources import GirderConnection, GirderCredentials

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    sensors=[assets.indentation_sensor],
    jobs=[assets.indentation_job, assets.summary_job],
    resources={
        "girder": GirderConnection(
            credentials=GirderCredentials(
                api_key=EnvVar("GIRDER_API_KEY"),
                api_url=EnvVar("GIRDER_API_URL"),
            )
        ),
    },
)
