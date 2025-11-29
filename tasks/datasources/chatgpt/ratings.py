import json
import re

import luigi

from tasks.lenient import LenientTask

from .chatgpt import MODEL_HIGH_QUALITY, ChatGptTask


class ExtractRating(ChatGptTask):
    """Prompts ChatGPT for a rating of a plant.

    Input: scientific name of plant (genus + species)
    Output: ChatGPT's text response to the prompt
    """

    scientific_name: str = luigi.Parameter()  # type: ignore

    def get_model(self):
        return MODEL_HIGH_QUALITY


class TransformRating(LenientTask):
    """Parses a plant rating from ChatGPT's response.

    Input: scientific name of plant (genus + species)
    Output: A JSON object with:
        something_rating: an integer between 1 and 10
    """

    task_namespace = "chatgpt"  # allows tasks of same name in diff packages
    scientific_name: str = luigi.Parameter()  # type: ignore

    def run_lenient(self):
        with self.input().open("r") as f:
            rating = self.parse_rating(f.read())
            rating_field_name = self.get_rating_field_name()
            result = {rating_field_name: rating}

            with self.output().open("w") as f:
                f.write(json.dumps(result, indent=4))

    def get_rating_field_name(self) -> str:
        raise NotImplementedError(
            "get_rating_field_name must be overridden by subclasses"
        )

    def parse_rating(self, raw_response: str) -> int:
        # Split the raw response into lines
        lines = raw_response.split("\n")

        # First, try to find the structured rating, as this is what
        # is usually returned.
        # Find the line which contains "rating:"
        for line in lines:
            if "rating:" in line.lower():
                # There could be spaces before rating or after the colon.
                # Split on the colon and trim any whitespace.
                rating_split = line.split(":")
                rating_str = rating_split[1].strip()

                try:
                    return int(rating_str)
                except ValueError:
                    print(f"Failed to parse: {rating_str}")
                    continue

        # Find any matching regexes which can capture the rating.  Try to parse it.
        # As the LLM finds more creative ways to not follow instructions, add to
        # this list.
        unstructured_regexes = [
            r"(\d) out of 10",
            r"(\d) on a scale from 1 to 10",
            r"(\d)/10",
            r"rate [a-zA-Z ]* as a (\d)",
        ]

        for unstructured_regex in unstructured_regexes:
            regex = re.compile(unstructured_regex)
            matches = regex.findall(raw_response)

            for match in matches:
                try:
                    return int(match)
                except ValueError:
                    continue

        raise ValueError(f"Could not parse rating from response: {raw_response}")


class ExtractPollinatorRating(ExtractRating):
    """Prompts ChatGPT for the pollinator rating of a plant.

    Input: scientific name of plant (genus + species)
    Output: ChatGPT's text response to the prompt
    """

    def output(self):
        return luigi.LocalTarget(
            f"data/raw/chatgpt/pollinator_rating/{self.scientific_name}.{self.get_model()}.txt"
        )

    def get_prompt(self) -> str:
        return f"""Your goal is to rate {self.scientific_name} compared to other plants 
with respect to how well it supports pollinators.  To do this, lets think step by step.

First, explain how well it supports the pollinators of an ecosystem.  Consider its 
contributions as a food source, shelter, and larval host. If it supports specific 
species, mention them. Also explain how it is deficient, if applicable.
        
Next, compare how well it does compared to other plants. 

Finally, rate how well it supports them on a scale from 
1-10 compared to other plants.

Your entire response will be formatted as follows, the 
'rating:' label is REQUIRED:
```
Your 2-4 sentence explanation.

Your 2-4 sentence comparison.

rating: Your integer rating from 1-10, compared to other
plants. 1-3 is average, 4-8 is for strong contributors,
9-10 is for the very best.
```

For example (the 'rating:' label is REQUIRED):
```
<plant name> are... (2-4 sentences)

Compared to other plants... (2-4 sentences)

rating: 3
"""


class TransformPollinatorRating(TransformRating):
    """Parses a plant's pollinator rating from ChatGPT's response.

    Input: scientific name of plant (genus + species)
    Output: A JSON object with:
        pollinator_rating: an integer between 1 and 10
    """

    task_namespace = "chatgpt"  # allows tasks of same name in diff packages

    def requires(self):
        return ExtractPollinatorRating(scientific_name=self.scientific_name)

    def output(self):
        return luigi.LocalTarget(
            f"data/transformed/chatgpt/pollinator_rating/{self.scientific_name}.json"
        )

    def get_rating_field_name(self) -> str:
        return "pollinator_rating"


class ExtractBirdRating(ExtractRating):
    """Prompts ChatGPT for the bird rating of a plant.

    Input: scientific name of plant (genus + species)
    Output: ChatGPT's text response to the prompt
    """

    def output(self):
        return luigi.LocalTarget(
            f"data/raw/chatgpt/bird_rating/{self.scientific_name}.{self.get_model()}.txt"
        )

    def get_prompt(self) -> str:
        return f"""Your goal is to rate {self.scientific_name} compared to other plants
with respect to how well it supports birds.  To do this, lets think step by step.

First, explain how well it supports the birds of an ecosystem.  Consider its
contributions as a food source, shelter, and nesting site. If it supports specific
species, mention them. Also explain how it is deficient, if applicable.

Next, compare how well it does compared to other plants.

Finally, rate how well it supports them on a scale from
1-10 compared to other plants.

Your entire response will be formatted as follows, the
'rating:' label is REQUIRED:
```
Your 2-4 sentence explanation.

Your 2-4 sentence comparison.

rating: Your integer rating from 1-10, compared to other
plants. 1-3 is average, 4-8 is for strong contributors,
9-10 is for the very best.
```

For example (the 'rating:' label is REQUIRED):
```
<plant name> are... (2-4 sentences)

Compared to other plants... (2-4 sentences)

rating: 3
"""


class TransformBirdRating(TransformRating):
    """Parses a plant's bird rating from ChatGPT's response.

    Input: scientific name of plant (genus + species)
    Output: A JSON object with:
        bird_rating: an integer between 1 and 10
    """

    task_namespace = "chatgpt"  # allows tasks of same name in diff packages

    def requires(self):
        return ExtractBirdRating(scientific_name=self.scientific_name)

    def output(self):
        return luigi.LocalTarget(
            f"data/transformed/chatgpt/bird_rating/{self.scientific_name}.json"
        )

    def get_rating_field_name(self) -> str:
        return "bird_rating"


class ExtractSpreadRating(ExtractRating):
    """Prompts ChatGPT for the spread rating of a plant.

    Input: scientific name of plant (genus + species)
    Output: ChatGPT's text response to the prompt
    """

    def output(self):
        return luigi.LocalTarget(
            f"data/raw/chatgpt/spread_rating/{self.scientific_name}.{self.get_model()}.txt"
        )

    def get_prompt(self) -> str:
        return f"""Your goal is to rate how aggressively {self.scientific_name}
spreads.  To do this, lets think step by step.

First, explain how aggressively it spreads.  Then, rate this aggressiveness
on a scale from 1 to 10.

Your entire response will be formatted as follows, the rating label is REQUIRED:
```
Your 3-5 sentence description.

rating: Your integer rating from 1-10, compared to other plants.
1-4 is doesn't spread much, 5-7 is for spreading noticably,
8-10 will be very difficult to control.
```

For example ('rating:' label is REQUIRED):
```
<plant name> spreads... (2-4 sentences)

rating: 3
```"""


class TransformSpreadRating(TransformRating):
    """Parses a plant's spread rating from ChatGPT's response.

    Input: scientific name of plant (genus + species)
    Output: A JSON object with:
        spread_rating: an integer between 1 and 10
    """

    task_namespace = "chatgpt"  # allows tasks of same name in diff packages

    def requires(self):
        return ExtractSpreadRating(scientific_name=self.scientific_name)

    def output(self):
        return luigi.LocalTarget(
            f"data/transformed/chatgpt/spread_rating/{self.scientific_name}.json"
        )

    def get_rating_field_name(self) -> str:
        return "spread_rating"


class ExtractDeerResistanceRating(ExtractRating):
    """Prompts ChatGPT for the deer resistance rating of a plant.

    Input: scientific name of plant (genus + species)
    Output: ChatGPT's text response to the prompt
    """

    def output(self):
        return luigi.LocalTarget(
            f"data/raw/chatgpt/deer_resistance_rating/{self.scientific_name}.{self.get_model()}.txt"
        )

    def get_prompt(self) -> str:
        return f"""Your goal is to rate the deer resistance of {self.scientific_name}.
To do this, lets think step by step.

First, explain how it resists deer.  Then, rate this resistance on a scale from 1 to 10.

Your entire response will be formatted as follows, the rating label is REQUIRED:
```
Your 3-5 sentence description.

rating: Your integer rating from 1-10, compared to other plants. 1-4 is not deer resistant,
4-6 is not preferred by deer, 7-10 deer will not eat.
```

For example: ('rating:' label is REQUIRED):
```
<plant name> is... (3-5 sentences)

rating: 3
```
"""


class TransformDeerResistanceRating(TransformRating):
    """Parses a plant's deer resistance rating from ChatGPT's response.

    Input: scientific name of plant (genus + species)
    Output: A JSON object with:
        deer_resistance_rating: an integer between 1 and 10
    """

    task_namespace = "chatgpt"  # allows tasks of same name in diff packages

    def requires(self):
        return ExtractDeerResistanceRating(scientific_name=self.scientific_name)

    def output(self):
        return luigi.LocalTarget(
            f"data/transformed/chatgpt/deer_resistance_rating/{self.scientific_name}.json"
        )

    def get_rating_field_name(self) -> str:
        return "deer_resistance_rating"
