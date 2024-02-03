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


class ExtractPollinatorRating(ExtractRating):
    """Prompts ChatGPT for the pollinator rating of a plant.

    Input: scientific name of plant (genus + species)
    Output: ChatGPT's text response to the prompt
    """

    scientific_name: str = luigi.Parameter()

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


class TransformPollinatorRating(TransformRating):
    """Parses a plant's pollinator rating from ChatGPT's response.

    Input: scientific name of plant (genus + species)
    Output: A JSON object with:
        pollinator_rating: an integer between 1 and 10
    """

    task_namespace = "chatgpt"  # allows tasks of same name in diff packages
    scientific_name: str = luigi.Parameter()

    def requires(self):
        return ExtractPollinatorRating(scientific_name=self.scientific_name)

    def output(self):
        return luigi.LocalTarget(
            f"data/transformed/chatgpt/pollinator_rating/{self.scientific_name}.json"
        )

    def get_rating_field_name(self) -> str:
        return "pollinator_rating"
