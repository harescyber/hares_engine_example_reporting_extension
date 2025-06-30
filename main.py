import json
import os
from time import time
from yapsy.IMultiprocessPlugin import IMultiprocessPlugin
from hares_engine.model.log import Logger
from hares_engine.util.functions import EngineUtil
from hares_engine.core.database import __db__

engine_util = EngineUtil()


class ExampleReportingExtension(IMultiprocessPlugin):
    def __init__(self, p):
        IMultiprocessPlugin.__init__(self, p)

        data = p.recv()
        self.config = data["config"]
        message = data["message"]

        __db__.register_extension(self.config)

        print(f"{message}")

    def run(self):
        logger = Logger(self.config.log, __name__.split(".")[-1])

        i = 0
        t = time()

        # Open log files
        bulk = ""

        for data in __db__.get_message():
            if data["type"] == "message":
                if time() - t > 3:
                    logger.debug(f"{i} items processed from {data['queue_length']}")
                    t = time()
                    if not os.path.exists(
                        engine_util.relative(__file__, self.config.log.dir)
                    ):
                        os.makedirs(engine_util.relative(__file__, self.config.log.dir))
                    if not os.path.exists(
                        engine_util.relative(__file__, self.config.log.dir, "log.txt")
                    ):
                        with open(
                            engine_util.relative(
                                __file__, self.config.log.dir, "log.txt"
                            ),
                            "w",
                        ) as f:
                            f.write("")

                    with open(
                        engine_util.relative(__file__, self.config.log.dir, "log.txt"),
                        "a",
                    ) as f:
                        f.write(bulk)
                        bulk = ""

                i += 1
                inference = __db__.retrieve("inference", data["data"])
                log = json.dumps(inference)
                bulk += log + "\n"
