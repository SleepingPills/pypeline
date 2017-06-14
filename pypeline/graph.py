import functools

from collections import namedtuple
from pypeline.context import Context
from pypeline.common import NodeDef, EdgeDef

__all__ = ["node", "pipe", "Graph"]


NamedFunc = namedtuple("NamedFunc", "func name")


def node(func, name=None):
    """
    Creates a custom named node.

    :param func: Node function.
    :param name: Node name.
    :return: Named node.
    """
    return NamedFunc(func, name)


def pipe(*args):
    """
    Construct a graph by pipelining the supplied nodes.

    :param args: One or more nodes.
    :return: Graph.
    """
    # Bail out early if there is only one item
    if len(args) == 1:
        return Graph(args)

    graph = Graph()
    graph._pipe(args)
    return graph


class BaseGraph(object):
    """
    Represents a sub-graph, a part of another graph. Sub-graphs are not intended to be
    used on their own, rather provide support machinery for manipulating graphs.
    """
    def __init__(self, prefix, downstream, upstream, root):
        """
        Constructs a new graph from the given items and named graphs..
        """
        self.__dict__["_items"] = {}
        self.__dict__["_prefix"] = prefix
        self.__dict__["_downstream"] = downstream
        self.__dict__["_upstream"] = upstream
        self.__dict__["_root"] = root

    def union(self, *graphs, **named_graphs):
        """
        Merges the supplied graphs into this graph.

        :param graphs: Graphs to merge.
        :param named_graphs: Named graphs to merge.
        """
        for item in graphs:
            self._store(item)

        for name, item in named_graphs.items():
            self._store(item, name=name)

    def pipe(self, *nodes):
        """
        Pipelines the supplied nodes. If a node is not in this graph, it
        will be added.

        NOTE: Pipelining graphs is not supported at the moment.

        :param args: One or more functions or nodes.
        """
        if len(nodes) < 2:
            raise ValueError("Provide at least two nodes to connect")

        self._pipe(nodes)

        return self

    def join(self, from_nodes, target):
        """
        Joins `from_nodes` to the `target` node.

        :param from_nodes: Source nodes.
        :param target: Target node
        """
        for from_node in from_nodes:
            self._pipe((from_node, target))

        return self

    def fan(self, from_node, targets):
        for target in targets:
            self._pipe((from_node, target))

        return self

    def _pipe(self, nodes):
        """
        Pipeline the supplied nodes together (in order).

        :param nodes: Nodes to pipeline. Nodes not in this graph will be added.
        """
        # Collect all vertices, handling special named parameter targets separately.
        vertices = []
        for item in nodes:
            if isinstance(item, EdgeDef):
                vertices.append(EdgeDef(self._store_node(item.node), item.param))
            else:
                vertices.append(EdgeDef(self._store_node(item), None))

        for i in range(1, len(vertices)):
            source = vertices[i - 1]
            target = vertices[i]

            self._downstream[source.node].append(target)
            self._upstream[target.node].append(EdgeDef(source.node, target.param))

    def _store(self, item, name=None):
        """
        Extract node data out from `item` in a robust manner and add it to the graph.

        :param item: Item containing node or graph data.
        :param name: Name of the item.
        """
        if isinstance(item, SubGraph):
            raise ValueError("Merging sub-graphs is not supported")
        elif isinstance(item, Graph):
            self._store_graph(item, name=name)
        else:
            self._store_node(item, name=name)

    def _store_graph(self, graph, name=None):
        """
        Merge the supplied blueprint into this blueprint, optionally prefixing all nodes
        with the `prefix` argument.

        :param graph: Sub-graph to merge.
        :param name: Store the sub-graph under the given name. Merge into the current namespace
                     otherwise.
        """
        if graph == self:
            raise ValueError("Cannot add graph into itself")
        if name is None:
            root = self
        else:
            # When a name is specified, create or merge into a new subgraph
            try:
                root = self._items[name]
                if isinstance(root, NodeDef):
                    raise ValueError("Graph already contains node by name %s" % name)
            except KeyError:
                root = SubGraph(self._prefix + (name,), self._downstream, self._upstream, self._root)
                self._items[name] = root

        # Function for recursively copying over the source graph structure to the target
        def _copy_structure(source_graph, target_graph, prefix):
            for key, value in source_graph._items.items():
                if isinstance(value, NodeDef):
                    target_graph._store_node(value)
                else:
                    new_prefix = prefix + (key,)

                    try:
                        new_graph = target_graph._items[key]
                    except KeyError:
                        target_graph._items[key] = new_graph = SubGraph(new_prefix,
                                                                        self._downstream,
                                                                        self._upstream,
                                                                        self._root)

                    _copy_structure(value, new_graph, new_prefix)

        _copy_structure(graph, root, root._prefix)

        def _copy_edges(source_edges, target_edges):
            for source, targets in source_edges.items():
                target_edge = target_edges[root._prefix + source]

                # Filter out edge definitions already present.
                # This needs to be done in two passes because there can be deliberately duplicate edge definitions
                # for nodes.
                rebased_targets = []
                for target in targets:
                    edge = EdgeDef(root._prefix + target.node, target.param)
                    if edge not in target_edge:
                        rebased_targets.append(edge)

                target_edge.extend(rebased_targets)

        _copy_edges(graph._downstream, self._downstream)
        _copy_edges(graph._upstream, self._upstream)

    def _store_node(self, item, name=None, args=None, kwargs=None):
        """
        Extract node data from `item` in a robust manner.

        :param item: Can be a node, function, curried function, lambda or any callable.
        :param name: Name to use for the node. If `None`, an attempt will be made to deduce it from `item`.
        :param args: Default positional arguments to the node function.
        :param kwargs: Default keyword arguments to the node function.
        :return:
        """
        if isinstance(item, NodeDef):
            if item.owner == self._root:
                # Don't re-add nodes already in this graph, just return the path
                return item.path
            else:
                # Node definitions from another graph need to be rebased
                return self._store_node_def(item.rebase(self._root, self._prefix))
        elif isinstance(item, functools.partial):
            if args is not None or kwargs is not None:
                raise ValueError("Extra arguments and nesting not supported for partial functions.")
            return self._store_node(item.func, name=name, args=item.args, kwargs=item.keywords)
        elif isinstance(item, NamedFunc):
            return self._store_node(item.func, item.name)
        elif callable(item):
            # Try to extract the node key if it is not known yet
            if name is None:
                if hasattr(item, "im_func"):
                    name = item.im_func.func_name
                elif hasattr(item, "func_name"):
                    name = item.func_name
                    if name == "<lambda>":
                        raise ValueError("Anonymous lambda functions are unsupported")
                else:
                    raise ValueError("Can't deduce name for %s" % item)

            # Ensure args is a tuple and not a mutable list
            if args is not None:
                args = tuple(args)

            return self._store_node_def(NodeDef(self._root, item, self._prefix, name, args, kwargs))
        else:
            raise ValueError("Unsupported node specification %s" % item)

    def _store_node_def(self, node_def):
        """
        Stores the node definition. Existing node definitions will be updated.

        :param node_def: New node definition to use.
        :return: Fully qualified path of the node.
        """
        node_name = node_def.name
        node_path = node_def.path

        try:
            existing_node = self._items[node_name]

            # Raise error in case the key denotes a sub-graph
            if isinstance(existing_node, SubGraph):
                raise ValueError("Attempted to override sub-graph %s with node" % ".".join(node_path))

            existing_node.update(node_def)
        except KeyError:
            self._items[node_name] = node_def
            self._downstream[node_path] = []
            self._upstream[node_path] = []

        return node_path

    def __getitem__(self, key):
        return self._items[key]

    def __setitem__(self, key, value):
        self._store(value, key)

    def __getattr__(self, key):
        try:
            return self._items[key]
        except KeyError:
            raise AttributeError(key)

    def __setattr__(self, key, value):
        if key in self.__dict__:
            self.__dict__[key] = value
        else:
            self[key] = value

    def __dir__(self):
        return self.__dict__.keys() + self._items.keys()


class SubGraph(BaseGraph):
    def __init__(self, prefix, downstream, upstream, root):
        """
        A sub-graph is a collection of items nested in another graph. SubGraphs are not meant
        to exist on their own - they are always contained in other SubGraphs or a root graph.

        :param prefix: Fully qualified prefix tuple - e.g. ("a", "b", "c").
        :param downstream: Downstream edge mapping of the root graph.
        :param upstream: Upstream edge mapping of the root graph.
        :param root: The root graph.
        """
        super(SubGraph, self).__init__(prefix, downstream, upstream, root)


class Graph(BaseGraph):
    def __init__(self, *items, **named_items):
        """
        A composable graph object for constructing data flow processors and transformation
        pipelines.

        To make a graph executable, a context needs to be constructed from it by calling
        the graph instance:

            context = my_graph(...some params...)

        :param items: Nodes and graphs to add to this graph.
        :param named_items: Named nodes and sub-graphs to add to this graph.
        """
        super(Graph, self).__init__((), {}, {}, self)

        for item in items:
            self._store(item)

        for name, item in named_items.items():
            self._store(item, name=name)

    def __call__(self, **kwargs):
        """
        Construct a new graph context from this graph.

        :param kwargs: Node and global parameters that will be passed to the graph. Any keys
                       that refer to nodes will be assumed to contain an `aim.graph.params`
                       tuple. Keys that do not refer to a node will be passed to all input
                       nodes with a parameter by that name.
        """
        return Context(self, kwargs)
