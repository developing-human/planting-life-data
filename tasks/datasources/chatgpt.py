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
        # Default to a cheap/fast model
        return MODEL_GPT_3_5

    def get_prompt(self):
        raise NotImplementedError("get_prompt must be overridden by subclasses")


class ExtractGrowingConditions(ChatGptTask):
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
    result = {}

    yes_answers = find_yes_answers(text, question_to_field.keys())
    for question, result_field in question_to_field.items():
        result[result_field] = question in yes_answers

    return result


def find_yes_answers(text: str, questions: list[str]) -> list[str]:
    prefix_regex = re.compile(r"[ ]*-[ ]*|\d\. *")

    answer_count = 0
    yes_answers = []
    for line in text.split("\n"):
        line = prefix_regex.sub("", line)

        for question in questions:
            if line.startswith(question):
                answer_count += 1
                if "yes" in line:
                    yes_answers.append(question)

    if answer_count != len(questions):
        raise ValueError("did not find all condition answers")

    if not yes_answers:
        raise ValueError("did not find any yes answers")

    return yes_answers
