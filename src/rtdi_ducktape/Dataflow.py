import logging
from logging import Logger
from typing import Union

from rtdi_ducktape.Loaders import Loader
from rtdi_ducktape.Metadata import Step, OperationalMetadata
from rtdi_ducktape.SQLUtils import empty


class Dataflow:

    def __init__(self, logger: Union[None, Logger] = None):
        self.nodes = []
        if logger is None:
            self.logger = logging.getLogger("Dataflow")
        else:
            self.logger = logger
        self.last_execution = None

    def add(self, step: Step):
        self.nodes.append(step)
        return step

    def start(self, duckdb):
        if len(self.nodes) > 0:
            self.last_execution = OperationalMetadata()
            self.nodes[0].start(duckdb)
            rows_loaded = 0
            for node in self.nodes:
                if isinstance(node, Loader) and node.last_execution is not None:
                    rows_loaded += node.last_execution.rows_processed
            self.last_execution.processed(rows_loaded)
            self.logger.info(f"Dataflow() - {self.last_execution}")