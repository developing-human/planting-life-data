import logging
import os
import sys

import luigi

from tasks.tables.images.generate import (
    GenerateImagesCsv,
    GenerateImagesSql,
    GenerateImagesWithoutHumanOverridesCsv,
)

logging.getLogger().setLevel(logging.WARN)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: python3 {sys.argv[0]} plants_list.txt")
        exit(1)

    plants_filename = sys.argv[1]

    # Remove output files from these tasks before generating
    # Always want fresh results when using script
    tasks_to_clear = [
        GenerateImagesSql(plants_filename=plants_filename),
        GenerateImagesWithoutHumanOverridesCsv(plants_filename=plants_filename),
        GenerateImagesCsv(plants_filename=plants_filename),
    ]

    for task in tasks_to_clear:
        path = task.output()[0].path
        if os.path.exists(path):
            os.remove(path)

    task = GenerateImagesSql(plants_filename=plants_filename)

    result = luigi.build(
        [task],
        workers=1,
        local_scheduler=True,
        log_level="WARNING",
    )

    if result:
        print("Done :)")
    else:
        print("Done, but :(")
