import sys
import luigi
from tasks.tables.plants.generate import GeneratePlantsCsv
import logging

logging.getLogger().setLevel(logging.WARN)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: python3 {sys.argv[0]} plants_list.txt")
        exit(1)

    result = luigi.build(
        [GeneratePlantsCsv(plants_filename=sys.argv[1])],
        workers=1,
        local_scheduler=True,
        log_level="WARNING",
    )

    if result:
        print("Done :)")
    else:
        print("Done, but :(")
