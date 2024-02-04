import luigi
import json
import re
from tasks.lenient import LenientTask
from .chatgpt import ChatGptTask, MODEL_GPT_4_TURBO


class TransformSize(LenientTask):
    """Parses a plant size from ChatGPT's response.

    Input: scientific name of plant (genus + species)
    Output: A JSON object with:
        field_name: a string representing the size
    """

    task_namespace = "chatgpt"  # allows tasks of same name in diff packages
    scientific_name: str = luigi.Parameter()

    def run_lenient(self):
        with self.input().open("r") as f:
            size = self.parse_size(f.read())
            size_field_name = self.get_size_field_name()
            result = {size_field_name: size}

            with self.output().open("w") as f:
                f.write(json.dumps(result, indent=4))

    def get_size_field_name(self) -> str:
        raise NotImplementedError(
            "get_size_field_name must be overridden by subclasses"
        )

    def parse_size(self, raw_response: str) -> int:
        pattern = r'[0-9]+[\'"]?-[0-9]+[\'"]'
        match = re.search(pattern, raw_response)
        if match:
            return match.group()
        else:
            print("Failed to parse size")
            raise ValueError(f"could not find measurement in: {raw_response}")


class ExtractHeight(ChatGptTask):
    """Prompts ChatGPT for the height of a plant.

    Input: scientific name of plant (genus + species)
    Output: ChatGPT's text response to the prompt
    """

    scientific_name: str = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            f"data/raw/chatgpt/height/{self.scientific_name}.{self.get_model()}.txt"
        )

    def get_prompt(self):
        return f"""How tall is {self.scientific_name}?  On the last line of your response, 
list only feet and inches using ' and \" for abbreviations.  Here are two examples:

```
<plant name> typically grows to a height of 10 to 20 feet.
10'-20'
```

```
<plant name> typically grows to a height of 18 to 24 inches.
18\"-24\"
```"""

    def get_model(self):
        return MODEL_GPT_4_TURBO


class TransformHeight(TransformSize):
    """Parses a plant's height from ChatGPT's response.

    Input: scientific name of plant (genus + species)
    Output: A JSON object with:
        height: a string like 2'-3' or 6"-8"
    """

    task_namespace = "chatgpt"  # allows tasks of same name in diff packages

    def requires(self):
        return ExtractHeight(scientific_name=self.scientific_name)

    def output(self):
        return luigi.LocalTarget(
            f"data/transformed/chatgpt/height/{self.scientific_name}.json"
        )

    def get_size_field_name(self) -> str:
        return "height"


class ExtractWidth(ChatGptTask):
    """Prompts ChatGPT for the width/spread of a plant.

    Input: scientific name of plant (genus + species)
    Output: ChatGPT's text response to the prompt
    """

    scientific_name: str = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            f"data/raw/chatgpt/width/{self.scientific_name}.{self.get_model()}.txt"
        )

    def get_prompt(self):
        return f""" What is {self.scientific_name}'s width or spread?  On the last line of your response, 
list only feet and inches using ' and \" for abbreviations.  Here are two examples:

```
<plant name>'s typically spread is 10 to 20 feet.
10'-20'
```

```
<plant name>'s typically spread is 18 to 24 inches.
18\"-24\"
```"""

    def get_model(self):
        return MODEL_GPT_4_TURBO


class TransformWidth(TransformSize):
    """Parses a plant's width from ChatGPT's response.

    Input: scientific name of plant (genus + species)
    Output: A JSON object with:
        width: a string like 2'-3' or 6"-8"
    """

    task_namespace = "chatgpt"  # allows tasks of same name in diff packages

    def requires(self):
        return ExtractWidth(scientific_name=self.scientific_name)

    def output(self):
        return luigi.LocalTarget(
            f"data/transformed/chatgpt/width/{self.scientific_name}.json"
        )

    def get_size_field_name(self) -> str:
        return "width"


class ExtractBloom(ChatGptTask):
    """Prompts ChatGPT for the blooming season of a plant.

    Input: scientific name of plant (genus + species)
    Output: ChatGPT's text response to the prompt
    """

    scientific_name: str = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            f"data/raw/chatgpt/bloom/{self.scientific_name}.{self.get_model()}.txt"
        )

    def get_prompt(self):
        return f"""In what season does {self.scientific_name} typically start blooming?
Choose one of: early spring, spring, late spring, early summer, summer, late summer,
early fall, fall, or late fall.  If it does not bloom, say 'does not bloom'."""

    def get_model(self):
        return MODEL_GPT_4_TURBO


class TransformBloom(LenientTask):
    """Parses a plant blooming season from ChatGPT's response.

    Input: scientific name of plant (genus + species)
    Output: A JSON object with:
        bloom: a string representing the size
    """

    task_namespace = "chatgpt"  # allows tasks of same name in diff packages
    scientific_name: str = luigi.Parameter()

    def requires(self):
        return ExtractBloom(scientific_name=self.scientific_name)

    def output(self):
        return luigi.LocalTarget(
            f"data/transformed/chatgpt/bloom/{self.scientific_name}.json"
        )

    def run_lenient(self):
        with self.input().open("r") as f:
            size = self.parse_bloom(f.read())
            result = {"bloom": size}

            with self.output().open("w") as f:
                f.write(json.dumps(result, indent=4))

    def parse_bloom(self, raw_response: str) -> int:
        seasons = [
            "early spring",
            "late spring",
            "spring",
            "early summer",
            "late summer",
            "summer",
            "early fall",
            "late fall",
            "fall",
            "early autumn",
            "late autumn",
            "autumn",
            "does not bloom",
        ]

        input_lc = raw_response.lower()
        for season in seasons:
            if season in input_lc:
                return season.replace("autumn", "fall").replace("does not bloom", "N/A")

        raise ValueError(f"could not find season in: {raw_response}")
