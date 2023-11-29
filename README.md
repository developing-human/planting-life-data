**Planting Life Data** collects data about native plants and organizes it to be
accessible to new or intermediate gardeners.  The collected data is used by
[Planting Life](https://planting.life), but may be useful for other projects
focused on native plants.

## Contributing
TODO: Describe how both non-technical and technical contributions may be made

## Design
Data is collected using an [ETL](https://en.wikipedia.org/wiki/Extract%2C_transform%2C_load) process driven by [Luigi](https://github.com/spotify/luigi).  Fetched data is cached to limit how often external services need to be hit.

* Extract: Raw data (HTML, JSON, etc) is extracted from datasources like [USDA](https://plants.usda.gov/), [Wildflower](https://www.wildflower.org/plants/), and [ChatGPT](https://chat.openai.com).
* Transform: The fetched data is parsed and transformed into the fields that will appear in the output.
* Load: The data is loaded into the generated CSV.

TODO: Write more about datasources / tasks / tables

### Setup
```bash
# Setup & activate the virtual env
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip3 install -r requirements.txt
```

### Usage
```bash
# Collect data about the plants in names_short.txt
python3 generate_plants_table.py names_short.txt

# Print the collected data to the terminal
cat cache/tables/plants-*.csv
```