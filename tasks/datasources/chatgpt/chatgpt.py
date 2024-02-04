import luigi
from tasks.lenient import LenientTask
import json
import openai
import re

MODEL_GPT_3_5 = "gpt-3.5-turbo"
MODEL_GPT_4 = "gpt-4"
MODEL_GPT_4_TURBO = "gpt-4-1106-preview"

SOURCE_NAME = "ChatGPT"


class ChatGptTask(LenientTask):
    """An abstract task for prompting ChatGPT.

    Implementors should override get_prompt and optionally get_model.
    """

    def run_lenient(self):
        client = openai.OpenAI()

        response = client.chat.completions.create(
            model=self.get_model(),
            messages=[
                {
                    "role": "system",
                    "content": "You are a discerning gardener who carefully follows formatting instructions",
                },
                {"role": "user", "content": self.get_prompt()},
            ],
            temperature=0.1,
        )

        with self.output().open("w") as f:
            f.write(response.choices[0].message.content)

    def get_model(self):
        """The ChatGPT model to use, defaulting to 3.5 because it's fast/cheap"""
        return MODEL_GPT_3_5

    def get_prompt(self):
        """The prompt to submit to ChatGPT"""
        raise NotImplementedError("get_prompt must be overridden by subclasses")


class ExtractGrowingConditions(ChatGptTask):
    """Prompts ChatGPT for the growing conditions of a plant.

    Input: scientific name of plant (genus + species)
    Output: ChatGPT's text response to the prompt
    """

    scientific_name: str = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            f"data/raw/chatgpt/conditions/{self.scientific_name}.{self.get_model()}.txt"
        )

    def get_prompt(self):
        return f"""Your goal is to answer six yes/no questions about shade
and moisture conditions where {self.scientific_name} will thrive.  First, describe
growing conditions where it will thrive in 40-50 words.

Then, use this format to answer the six questions:
- low moisture? yes/no
- medium moisture? yes/no
- high moisture? yes/no
- full shade? yes/no
- partial sun? yes/no
- full sun? yes/no

For example:
```
40-50 words about conditions where it thrives.

- low moisture? no
- medium moisture? yes
- high moisture? yes
- full shade? yes
- partial sun? no
- full sun? no
```
"""

    def get_model(self):
        return MODEL_GPT_4_TURBO


class TransformMoisture(LenientTask):
    """Parses a plant's moisture preferences from ChatGPT's growing condition response.

    Input: scientific name of plant (genus + species)
    Output: A JSON object with:
        low_moisture: Will this plant grow in low moisture?
        medium_moisture: Will this plant grow in medium moisture?
        high_moisture: Will this plant grow in high moisture?
        moisture_source: Always set to "ChatGPT"
        moisture_source_detail: The ChatGPT model which was used
    """

    task_namespace = "chatgpt"  # allows tasks of same name in diff packages
    scientific_name: str = luigi.Parameter()

    def requires(self):
        return ExtractGrowingConditions(scientific_name=self.scientific_name)

    def output(self):
        return luigi.LocalTarget(
            f"data/transformed/chatgpt/moisture/{self.scientific_name}.json"
        )

    def run_lenient(self):
        question_to_field = {
            "low moisture": "low_moisture",
            "medium moisture": "medium_moisture",
            "high moisture": "high_moisture",
        }

        with self.input().open("r") as f:
            result = build_condition_result(f.read(), question_to_field)
            result["moisture_source"] = SOURCE_NAME
            result["moisture_source_detail"] = self.requires().get_model()

            with self.output().open("w") as f:
                f.write(json.dumps(result, indent=4))


class TransformShade(LenientTask):
    """Parses a plant's shade preferences from ChatGPT's growing condition response.

    Input: scientific name of plant (genus + species)
    Output: A JSON object with:
        full_sun: Will this plant grow in full sun?
        part_shade: Will this plant grow in part shade?
        full_shade: Will this plant grow in full shade?
        shade_source: Always set to "ChatGPT"
        shade_source_detail: The ChatGPT model which was used
    """

    task_namespace = "chatgpt"  # allows tasks of same name in diff packages
    scientific_name: str = luigi.Parameter()

    def requires(self):
        return ExtractGrowingConditions(scientific_name=self.scientific_name)

    def output(self):
        return luigi.LocalTarget(
            f"data/transformed/chatgpt/shade/{self.scientific_name}.json"
        )

    def run_lenient(self):
        question_to_field = {
            "full shade": "full_shade",
            "partial sun": "part_shade",
            "full sun": "full_sun",
        }

        with self.input().open("r") as f:
            result = build_condition_result(f.read(), question_to_field)
            result["shade_source"] = SOURCE_NAME
            result["shade_source_detail"] = self.requires().get_model()

            with self.output().open("w") as f:
                f.write(json.dumps(result, indent=4))


def build_condition_result(
    text: str, question_to_field: dict[str, str]
) -> dict[str, bool]:
    """Parses the given text into a dict of True/False answers to each question"""
    result = {}

    yes_answers = find_yes_answers(text, question_to_field.keys())
    for question, result_field in question_to_field.items():
        result[result_field] = question in yes_answers

    return result


def find_yes_answers(text: str, questions: list[str]) -> list[str]:
    """Parses the given text into a list of the questions with yes answers"""
    # Support prefixes like:
    # - foo
    #    - foo
    #  -    foo
    # 1. foo
    # 2.    foo
    prefix_regex = re.compile(r"[ ]*-[ ]*|\d\. *")

    answer_count = 0
    yes_answers = []
    for line in text.split("\n"):
        # Remove the prefix
        line = prefix_regex.sub("", line)

        # Find lines which start with question text
        # and also contain "yes"
        for question in questions:
            if line.startswith(question):
                answer_count += 1

                # Does "yes" appear in the line after removing the question?
                # Question is removed to protect against questions which contain "yes"
                if "yes" in line.replace(question, ""):
                    yes_answers.append(question)

    # If not all questions are answered, the input text was invalid
    if answer_count != len(questions):
        raise ValueError("did not find all condition answers")

    # If nothing is answered yes, the response is invalid
    # This is context sensitive to the moisture/shade questions where
    # at least one from each category should be yes.
    if not yes_answers:
        raise ValueError("did not find any yes answers")

    return yes_answers


class ExtractRating(ChatGptTask):
    """Prompts ChatGPT for a rating of a plant.

    Input: scientific name of plant (genus + species)
    Output: ChatGPT's text response to the prompt
    """

    scientific_name: str = luigi.Parameter()

    def get_model(self):
        return MODEL_GPT_4_TURBO


class TransformRating(LenientTask):
    """Parses a plant rating from ChatGPT's response.

    Input: scientific name of plant (genus + species)
    Output: A JSON object with:
        something_rating: an integer between 1 and 10
    """

    task_namespace = "chatgpt"  # allows tasks of same name in diff packages
    scientific_name: str = luigi.Parameter()

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

    def get_prompt(self):
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

    def get_prompt(self):
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

    def get_prompt(self):
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

    def get_prompt(self):
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
