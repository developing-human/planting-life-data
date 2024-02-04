import luigi
import traceback
import logging


class StrictError(Exception):
    pass


class LenientTask(luigi.Task):
    """Tasks which extend this may throw errors without being "failed".  Instead, they
    create a blank file.  To fail, they may raise a StrictError.
    """

    def run(self):
        try:
            self.run_lenient()
        except StrictError as e:
            raise e
        except Exception as e:
            logging.warning(
                f"{type(self).__module__}.{type(self).__name__}: "
                f"Creating blank output after task failed with error: {e}",
            )
            logging.debug(traceback.format_exc())

            # output may be one item or a list.  convert to list.
            outputs = self.output()
            if not isinstance(outputs, list):
                outputs = [outputs]

            # write blank to each output
            # not writing causes downstream tasks to fail
            for output in outputs:
                with output.open("w") as _:
                    pass

    def run_lenient(self):
        raise NotImplementedError("Subclasses should implement this!")
