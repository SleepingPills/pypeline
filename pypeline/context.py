import inspect

from collections import namedtuple
from pypeline.common import NodeDef


_params = namedtuple("_params", "args, kwargs")


def group(**kwargs):
    """
    Construct a parameter group for setting sub-graph node parameters.

    :param kwargs: Global or node specific keyword arguments.
    :return: Parameter group.
    """
    return kwargs


def params(*args, **kwargs):
    """
    Construct a parameter tuple for nodes.

    :param args: Positional arguments.
    :param kwargs: Keyword arguments.
    :return: Parameter container.
    """
    return _params(args, kwargs)


class NodeArgSpec(object):
    def __init__(self, args, varargs, keywords):
        """
        Contains arg specs (position and keyword arguments).

        :param args: Positional arguments
        :param varargs: Name of the varargs container.
        :param keywords: Name of hte keyword args container.
        """
        self.args = args
        self._args_set = set(args)
        self.varargs = varargs
        self.keywords = keywords

    def __contains__(self, item):
        return item in self._args_set


class NodeStateBase(object):
    def __init__(self, name, func):
        """
        Base class for node state objects. Provides basic functionality for attaching upstream and
        downstream nodes, evaluating and invalidating the node and all dependent nodes.

        :param name: Name of the node.
        :param func: Function object containin node logic.
        """
        self.name = name
        self.func = func
        self.upstream = []
        self.downstream = []
        self._cache = None
        self._dirty = True

    def set(self, *args, **kwargs):
        """
        Sets parameters for the node. All downstream nodes will be invalidated and
        recomputed as needed.

        :param args: Positional arguments.
        :param kwargs: Keyword arguments.
        """
        self._set_params(args, kwargs)

    @property
    def val(self):
        """
        :return: Evaluate and/or return the cached results.
        """
        return self._eval_cached()

    def __call__(self, *args, **kwargs):
        """
        Evaluates the node with the supplied parameters, without caching the results.

        :param args: Positional arguments.
        :param kwargs: Keyword arguments.
        :return: Result.
        """
        return self._eval(args, kwargs)

    def _add_upstream(self, node):
        """
        Link an upstream node.

        :param node: Node state object.
        """
        self.upstream.append(node)

    def _add_downstream(self, node):
        """
        Link a downstream node.

        :param node: Node state object.
        """
        self.downstream.append(node)

    def _invalidate(self):
        """
        If this node is NOT dirty, invalidate it and all downstream nodes.
        """
        if not self._dirty:
            self._dirty = True
            self._cache = None

            for node in self.downstream:
                node._invalidate()

    def _set_params(self, args, kwargs):
        """
        Set the node parameters and invalidate this node and the dependent nodes.

        :param args: Position arguments.
        :param kwargs: Keyword arguments.
        """
        raise NotImplementedError("set_params")

    def _eval(self, args, kwargs):
        """
        Evaluate this node using the supplied arguments.

        :param args: Positional arguments.
        :param kwargs: Keyword arguments.
        :return: Evaluation result.
        """
        raise NotImplementedError("eval")

    def _eval_cached(self):
        """
        Evaluate the node if no cached result is available. Return the cached result otherwise.

        :return: Evaluation result.
        """
        raise NotImplementedError("eval_cached")


class NodeState(NodeStateBase):
    def __init__(self, name, func, args, kwargs):
        """
        Maintains node evaluation state for basic node types.

        :param name: Name of the node.
        :param func: A function, method or callable class instance.
        :param args: Positional arguments that will be passed to the function for evaluation.
        :param kwargs: Keyword arguments that will be passed to the function for evaluation.
        """
        super(NodeState, self).__init__(name, func)

        # If the func object is not a method or a function, assume it is a callable class
        if inspect.isclass(type(func)) and not inspect.ismethod(func) and not inspect.isfunction(func):
            self.func = func.__call__

        args_spec = inspect.getargspec(self.func)
        args_spec_args = args_spec.args

        # Exclude `self` for class methods
        if inspect.ismethod(self.func):
            args_spec_args = args_spec_args[1:]

        self._args_spec = NodeArgSpec(args_spec_args, args_spec.varargs, args_spec.keywords)

        self._args = args
        self._kwargs = kwargs

    def update(self, **kwargs):
        self._set_kwargs(kwargs, replace=False)

        self._invalidate()

    def _set_params(self, args, kwargs):
        """
        Set the node parameters and invalidate this node and the dependent nodes.

        :param args: Position arguments.
        :param kwargs: Keyword arguments.
        """
        self._set_args(args)
        self._set_kwargs(kwargs)

        self._invalidate()

    def _set_args(self, args):
        """
        Set positional arguments.

        :param args: Positional arguments.
        """
        self._args = args

    def _set_kwargs(self, kwargs, replace=True):
        """
        Set keyword arguments.

        :param kwargs: Keyword arguments.
        :param replace: Whether the existing argument is replaced or updated.
        """
        if replace:
            self._kwargs = {}

        # Filter out any arguments not in the arg spec if there is no kwargs catch-all
        if self._args_spec.keywords is None:
            for k, v in kwargs.iteritems():
                if k in self._args_spec:
                    self._kwargs[k] = v
        else:
            self._kwargs.update(kwargs)

    def _eval(self, args, kwargs):
        """
        Evaluate this node using the supplied arguments.

        :param args: Positional arguments.
        :param kwargs: Keyword arguments.
        :return: Evaluation result.
        """
        combined_args = []
        combined_kwargs = kwargs

        for upstream_node in self.upstream:
            incoming = upstream_node._eval_cached()

            # If the output is a `_params` object then use its contents as function arguments
            # to the current node function.
            if isinstance(incoming, _params):
                combined_args.extend(incoming.args)
                combined_kwargs.update(incoming.kwargs)
            else:
                combined_args.append(incoming)

        combined_args.extend(args)

        return self.func(*combined_args, **combined_kwargs)

    def _eval_cached(self):
        """
        Evaluate the node if no cached result is available. Return the cached result otherwise.

        :return: Evaluation result.
        """
        if self._dirty:
            self._cache = self._eval(self._args, self._kwargs.copy())
            self._dirty = False

        return self._cache


class ParamTargetNodeStateWrapper(object):
    def __init__(self, param_name, state):
        """
        Wrapper for nodes connected through edges targetting a particular parameter.

        :param param_name: Parameter name to recieve the upstream node result.
        :param state: Node state object to wrap.
        """
        self._param_name = param_name
        self._state = state

    def _eval_cached(self):
        """
        Evaluate the node if no cached result is available. Return the cached result otherwise.

        :return: Evaluation result.
        """
        return _params((), {self._param_name: self._state._eval_cached()})


class NodeGroup(object):
    def __init__(self):
        """
        Represents a group of nodes and sub-groups in an evaluation context.
        """
        self._items = {}

    def set(self, **kwargs):
        """
        Set global and node specific parameters. Keywords that map to nodes are expected to be
        supplied as a params tuple and take precedence before global parameters.

        Example:

            my_graph.set(global_par=5, node_specific=params(1, 2, 3, keyword="bla"))

        Global parameters are applied only to input nodes (nodes with no upstream edges) and
        only when the node function accepts an argument by that name.

        :param kwargs: Global and node specific parameters.
        """
        self._set_params(kwargs)

    def _set_params(self, kwargs):
        """
        Set global and specific parameters.

        :param kwargs: Global and node specific parameters.
        """
        global_params = {}
        spec_params = {}

        # Split out globally applied and node specific parameters.
        for key, value in kwargs.iteritems():
            if key in self._items:
                spec_params[key] = value
            else:
                global_params[key] = value

        # Apply the global parameters first as the specific ones are meant to override them.
        if len(global_params) > 0:
            self._set_global_params(global_params)
        if len(spec_params) > 0:
            self._set_params_spec(spec_params, global_params)

    def _set_global_params(self, global_params):
        """
        Recursively set global params on this node group and any sub-groups.

        :param global_params: Global parameters as a dictionary.
        """
        for item in self._items.values():
            if isinstance(item, NodeState):
                item._set_kwargs(global_params, replace=False)
                item._invalidate()
            else:
                item._set_global_params(global_params)

    def _set_params_spec(self, spec_params, global_params):
        """
        Recursively set specific parameters on this node group and any sub-group.

        :param spec_params: Node specific parameters or a hierarchical dictionary containing specific parameters.
        :param global_params: Global parameters that will be also applied.
        """
        for item_key, item_params in spec_params.iteritems():
            if isinstance(item_params, _params):
                self[item_key]._set_params(item_params.args, dict(global_params, **item_params.kwargs))
            elif isinstance(item_params, dict):
                self[item_key]._set_params_spec(item_params, global_params)
            else:
                raise ValueError("Unsupported parameter specification `%s`" % type(item_params))

    def _set_item(self, key, value):
        """
        Set the item with the given key to the given value.

        :param key: Name of the item.
        :param value: Value of the item.
        """
        self._items[key] = value

    def __getitem__(self, item):
        return self._items[item]

    def __getattr__(self, key):
        try:
            return self._items[key]
        except KeyError:
            raise AttributeError(key)

    def __dir__(self):
        return self.__dict__.keys() + self._items.keys()


class Context(NodeGroup):
    def __init__(self, graph_blueprint, kwargs):
        """
        Represents a graph context. Nodes evaluated by call will not be cached (their
        dependencies will be):

            my_graph.my_node() # Evaluate node without caching its value
            my_graph.my_node(1, 2, 3, keyword="moof") # Evaluate node with supplied parameters

        To evaluate and cache a node, use the `val` property:

            my_graph.my_node.val # Get cached value if available, evaluate and cache otherwise

        Note: unlike graphs, the context structure is immutable.

        :param graph_blueprint: Graph serving as a blueprint for the new context.
        :param kwargs: Initial parameters. Can be global or node specific parameters. Refer to
                       the `Context.set` method for details.
        """
        super(Context, self).__init__()

        self._nodes = {}

        downstream = graph_blueprint._downstream
        upstream = graph_blueprint._upstream

        def _walk_graph(graph, target_group):
            for key, value in graph._items.iteritems():
                if isinstance(value, NodeDef):
                    state = NodeState(key, value.func, value.args, value.kwargs.copy())
                    target_group._set_item(key, state)
                    self._nodes[value.path] = state
                else:
                    new_group = NodeGroup()
                    target_group._set_item(key, new_group)
                    _walk_graph(value, new_group)

        _walk_graph(graph_blueprint, self)

        def _parse_edges(edges, add_func):
            for source, targets in edges.iteritems():
                for target in targets:
                    add_func(self._nodes[source], target)

        _parse_edges(downstream, lambda source, target: source._add_downstream(self._nodes[target.node]))

        def _upstream_wire(source, target):
            target_node = self._nodes[target.node]
            # Edges that wire node output to specific parameters need to be wrapped.
            source._add_upstream(target_node if target.param is None else ParamTargetNodeStateWrapper(target_node,
                                                                                                      target.param))

        _parse_edges(upstream, _upstream_wire)

        self._set_params(kwargs)