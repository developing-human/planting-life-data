import luigi
from tasks.lenient import LenientTask
import json
import re
from .chatgpt import ChatGptTask, MODEL_HIGH_QUALITY, SOURCE_NAME


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

    def get_prompt(self) -> str:
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

    def get_model(self) -> str:
        return MODEL_HIGH_QUALITY


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
