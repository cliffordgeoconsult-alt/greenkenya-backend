"""Set during Celery `prewarm_forests_bundle` to detect accidental heavy GEE paths."""

_bundle_active = False


def prewarm_bundle_begin():
    global _bundle_active
    _bundle_active = True


def prewarm_bundle_end():
    global _bundle_active
    _bundle_active = False


def is_prewarm_bundle_active():
    return _bundle_active
