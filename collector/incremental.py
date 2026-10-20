"""Compatibility entrypoint for tauceti_tools.collector.incremental."""
import importlib
import runpy
import sys

if __name__ == "__main__":
    runpy.run_module("tauceti_tools.collector.incremental", run_name="__main__")
else:
    sys.modules[__name__] = importlib.import_module("tauceti_tools.collector.incremental")
