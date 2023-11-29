**Planting Life Data** collects data about native plants and organizes it to be
accessible to new or intermediate gardeners.  The collected data is used by
[Planting Life](https://planting.life), but may be useful for other projects
focused on native plants.

## Contributing
TODO: Describe how both non-technical and technical contributions may be made

## Design
TODO: Describe the high level design of the project

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