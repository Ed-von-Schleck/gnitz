# gnitz/backend/registry.py
# Backend factory registry. Populated at application startup.
# Used by tooling and alternative backends; not required by the VM itself.

_registry = {}

def register(name, table_class):
    _registry[name] = table_class

def create(name, directory, table_name, schema, **kwargs):
    if name not in _registry:
        raise KeyError("Unknown backend: %s" % name)
    return _registry[name](directory, table_name, schema, **kwargs)
