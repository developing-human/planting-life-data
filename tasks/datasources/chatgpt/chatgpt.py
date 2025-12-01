import openai

from tasks.lenient import LenientTask

MODEL_LOW_QUALITY = "gpt-5-nano"
MODEL_HIGH_QUALITY = "gpt-5-mini"

SOURCE_NAME = "ChatGPT"


class ChatGptTask(LenientTask):
    """An abstract task for prompting ChatGPT.

    Implementors should override get_prompt and optionally get_model.
    """

    def run_lenient(self):
        print("extract from chatgpt (api call)")
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
        )

        with self.output().open("w") as f:
            f.write(response.choices[0].message.content)

    def get_model(self) -> str:
        """The ChatGPT model to use, defaulting to low quality because it's fast/cheap"""
        return MODEL_LOW_QUALITY

    def get_prompt(self) -> str:
        """The prompt to submit to ChatGPT"""
        raise NotImplementedError("get_prompt must be overridden by subclasses")
