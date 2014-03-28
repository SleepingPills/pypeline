from functools import wraps
from pypeline.graph import Graph, node, pipe
from pypeline.context import params, group

__all__ = ["Graph", "node", "pipe", "params", "group"]


def in_out_dict(func):
    """
    Auto unwrap and wrap the first argument to the decorated function in case it is a dict.

    :param func: Function to decorate
    :return: Decorated function
    """
    @wraps(func)
    def _wrapper(data, *args, **kwargs):
        # Unpack the data in case it is a dict
        if isinstance(data, dict):
            # Return value should be the same type as the input
            result = type(data)()
            for k, v in data.iteritems():
                result[k] = func(v, *args, **kwargs)
            return result

        return func(data, *args, **kwargs)

    return _wrapper