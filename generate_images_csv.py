import logging
import os
import sys
import luigi
from tasks.tables.images.generate import GenerateImagesCsv

logging.getLogger().setLevel(logging.WARN)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: python3 {sys.argv[0]} plants_list.txt")
        exit(1)

    task = GenerateImagesCsv(plants_filename=sys.argv[1])

    # Remove output file before generating
    # Always want fresh results when using script
    path = task.output().path
    if os.path.exists(path):
        os.remove(path)

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
