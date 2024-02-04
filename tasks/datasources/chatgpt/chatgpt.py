from tasks.lenient import LenientTask
import openai

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

    def get_model(self) -> str:
        """The ChatGPT model to use, defaulting to 3.5 because it's fast/cheap"""
        return MODEL_GPT_3_5

    def get_prompt(self) -> str:
        """The prompt to submit to ChatGPT"""
        raise NotImplementedError("get_prompt must be overridden by subclasses")
