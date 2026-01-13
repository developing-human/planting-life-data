import logging
import os
import sys

import luigi

from tasks.datasources.plantinglife import (
    ExtractImages,
    ExtractPlantIds,
    ExtractPlants,
    ExtractZipcodesPlants,
    TransformSpecificPlantIds,
)
from tasks.generate import GenerateAllSql
from tasks.tables.images.generate import (
    GenerateImagesCsv,
    GenerateImagesSql,
    GenerateImagesWithoutHumanOverridesCsv,
)
from tasks.tables.plant_zipcodes.generate import (
    GeneratePlantsZipcodesCsv,
    GeneratePlantsZipcodesSqlDiff,
)
from tasks.tables.plants.generate import GeneratePlantsCsv, GeneratePlantsSql

logging.getLogger().setLevel(logging.WARN)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: python3 {sys.argv[0]} plants_list.txt")
        exit(1)

    plants_filename = sys.argv[1]

    # Remove output files from these tasks before generating
    # Always want fresh results when using script
    tasks_to_clear = [
        ExtractImages(),
        ExtractPlantIds(),
        ExtractPlants(),
        ExtractZipcodesPlants(),
        GenerateAllSql(plants_filename=plants_filename),
        GenerateImagesCsv(plants_filename=plants_filename),
        GenerateImagesSql(plants_filename=plants_filename),
        # i currently need to run ulimit -n 2048 for this to work, but i can fix it with a small refactor, I think.
        GenerateImagesWithoutHumanOverridesCsv(plants_filename=plants_filename),
        GeneratePlantsCsv(plants_filename=plants_filename),
        GeneratePlantsSql(plants_filename=plants_filename),
        # location data takes a while to rebuild, is it worth rebuilding?
        GeneratePlantsZipcodesCsv(plants_filename=plants_filename),
        GeneratePlantsZipcodesSqlDiff(plants_filename=plants_filename),
        TransformSpecificPlantIds(plants_filename=plants_filename),
    ]

    for task in tasks_to_clear:
        path = task.output()[0].path
        if os.path.exists(path):
            os.remove(path)

    tasks = [GenerateAllSql(plants_filename=plants_filename)]

    result = luigi.build(
        tasks,
        workers=1,
        local_scheduler=True,
        log_level="INFO",
    )

    if result:
        print("Done :)")
    else:
        print("Done, but :(")
