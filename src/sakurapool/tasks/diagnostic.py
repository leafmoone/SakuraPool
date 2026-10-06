"""Bounded diagnostics from identities of explicitly known installed functions."""
import json
from types import FunctionType

SAFE_TYPES = {kind: kind.__name__ for kind in (
    Exception, RuntimeError, ValueError, TypeError, AttributeError,
    KeyError, IndexError, OSError, FileNotFoundError, PermissionError,
    AssertionError, OverflowError, MemoryError, ImportError,
)}


def _trusted_codes():
    from ..storage.production import RustProductionTransport
    from . import cli, pipeline, runner, store

    # Fixed product call sites only; retain actual code references and compare by
    # identity. Never inspect arbitrary objects/descriptors or compiled sources.
    functions = (
        (vars(cli).get('command'), 'cli.py', 'command'),
        (vars(runner).get('run_task'), 'runner.py', 'run_task'),
        (vars(runner).get('preflight'), 'runner.py', 'preflight'),
        (vars(runner).get('reconcile'), 'runner.py', 'reconcile'),
        (vars(runner).get('verify_delivery'), 'runner.py', 'verify_delivery'),
        (vars(pipeline).get('run_pipeline'), 'pipeline.py', 'run_pipeline'),
        (vars(store.TaskDB).get('validate_plan'), 'store.py', 'validate_plan'),
        (vars(RustProductionTransport).get('predict_warm'), 'production.py', 'predict_warm'),
        (vars(RustProductionTransport).get('clone'), 'production.py', 'clone'),
        (vars(RustProductionTransport).get('enable_persistent'),
         'production.py', 'enable_persistent'),
    )
    return {id(function.__code__): (function.__code__, filename, name)
            for function, filename, name in functions if type(function) is FunctionType}


def safe_diagnostic(error):
    """Never read text/locals/chains; fixed fallback if diagnostics fail."""
    fallback = {'exception_type': 'untrusted_exception', 'frames': []}
    try:
        trusted = _trusted_codes()
        name = SAFE_TYPES.get(type(error), 'untrusted_exception')
        frames = []
        trace = error.__traceback__
        while trace is not None:
            code = trace.tb_frame.f_code
            match = trusted.get(id(code))
            if match is not None and match[0] is code:
                frame = {'file': match[1], 'function': match[2],
                         'line': min(max(trace.tb_lineno, 0), 2147483647)}
            else:
                frame = {'file': 'untrusted', 'function': 'untrusted', 'line': 0}
            frames.append(frame)
            if len(frames) > 32:
                del frames[0]
            trace = trace.tb_next
        result = {'exception_type': name, 'frames': frames}
        # Leave >1KiB for fixed outer CLI fields; whole output remains <=8KiB.
        if len(json.dumps(result, ensure_ascii=True).encode('ascii')) > 7168:
            return fallback
        return result
    except BaseException:
        return fallback
