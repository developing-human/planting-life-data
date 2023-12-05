**Planting Life Data** organizes information about native plants to be
accessible to gardeners. This is used by
[Planting Life](https://planting.life), but may be useful for other projects
focused on native plants.

The collected data will make it easy to answer questions such as:

- What plants are native near me?
- Will it grow in the shade?
- How tall is it?
- Will it get eaten by deer?
- Will it take over my garden?

## Contributing

If you're considering helping out with this project, thank you! You're awesome!

Two big ways you can help out are:

1. Plant enthusiasts: Suggesting new or better places to find information on plants
2. Programmers: Implementing the fetching/parsing of data for fields or sources which aren't yet supported

To make a suggestion, please create an [issue](https://github.com/developing-human/planting-life-data/issues).

TODO: Add link to slack.

## Design

Data is collected using an [ETL](https://en.wikipedia.org/wiki/Extract%2C_transform%2C_load) process
driven by [Luigi](https://github.com/spotify/luigi). Fetched data is cached to limit how often
external services need to be used.

- Extract: Raw data (HTML, JSON, etc) is extracted from datasources like [USDA](https://plants.usda.gov/),
  [Wildflower](https://www.wildflower.org/plants/), and [ChatGPT](https://chat.openai.com).
- Transform: The fetched data is parsed and transformed into the fields that will appear in the output.
- Load: The data is loaded into the generated CSV.

TODO: Write more about datasources / tasks / tables

## Setup

```bash
# Setup & activate the virtual env
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip3 install -r requirements.txt
```

## Usage

```bash
# Collect data about the plants in short.txt
python3 generate_plants_csv.py data/in/scientific-names/short.txt

# Print the collected data to the terminal
cat data/out/plants.csv
```
