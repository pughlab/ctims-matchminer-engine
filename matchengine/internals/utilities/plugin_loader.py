from __future__ import annotations

import logging
import sys

import inspect
import importlib
from pathlib import Path


log = logging.getLogger('matchengine.utilities')

class PluginLoadError(Exception):
    pass

def find_plugins(plugin_dir, base_classes):
    """
    Import a directory as a package and find any subclasses of the base_classes.
    """

    log.info(f"Loading plugins from {plugin_dir}")

    plugin_dir = Path(plugin_dir)
    init_file = plugin_dir / '__init__.py'
    if not init_file.is_file():
        raise PluginLoadError(f"Could not find plugin module file: {init_file}")

    saved_modules = set(sys.modules)
    try:
        spec = importlib.util.spec_from_file_location("__plugins", init_file)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module 
        spec.loader.exec_module(module)
    finally:
        new_modules = set(sys.modules)
        for mod in (new_modules - saved_modules):
            del sys.modules[mod]

    found_plugins = {cl: [] for cl in base_classes}

    items = [
        getattr(module, name)
        for name in dir(module)
    ]

    for item in items:
        if inspect.isclass(item):
            for k in found_plugins.keys():
                if issubclass(item, k) and not issubclass(k, item):
                    found_plugins[k].append(item)

    results = {}
    for base_class in sorted(base_classes, key=str):
        values = found_plugins[base_class]
        if len(values) > 1:
            raise PluginLoadError(f"Multiple plugins of kind {base_class.__name__} found")
        elif len(values) == 1:
            log.info(f"Loading {base_class.__name__} from plugin {values[0].__name__}")
            results[base_class] = values[0]
        else:
            log.info(f"Loading default {base_class.__name__}")
            results[base_class] = base_class

    return results
