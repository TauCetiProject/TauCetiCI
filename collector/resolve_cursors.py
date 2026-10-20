"""Compatibility entrypoint for tauceti_tools.collector.resolve_cursors."""
import importlib
import runpy
import sys

if __name__ == "__main__":
    runpy.run_module("tauceti_tools.collector.resolve_cursors", run_name="__main__")
else:
    sys.modules[__name__] = importlib.import_module("tauceti_tools.collector.resolve_cursors")
