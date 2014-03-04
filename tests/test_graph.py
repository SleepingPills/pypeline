from pytest import raises
from functools import partial

from pypeline.graph import node, Graph, EdgeDef


def _node_def_equals(candidate, graph, func, path, args, kwargs):
    return (candidate.owner == graph and candidate.func == func and candidate.path == path
            and candidate.args == args and candidate.kwargs == kwargs)


def a():
    pass


def b():
    pass


def c():
    pass


def d():
    pass


def test_node():
    assert node(a) == (a, None)


def test_node_callable():
    class _Cls(object):
        def __call__(self):
            pass

    inst = _Cls()

    g = Graph(node(inst, "moof"))

    assert _node_def_equals(g.moof, g, inst, ("moof",), (), {})


def test_node_method():
    class _Cls(object):
        def moof(self):
            pass

    inst = _Cls()

    g = Graph(inst.moof)

    assert _node_def_equals(g.moof, g, inst.moof, ("moof",), (), {})


def test_anon_lambda_fail():
    with raises(ValueError):
        Graph(lambda: 5)


def test_anon_callable_fail():
    class _Cls(object):
        def __call__(self):
            pass

    with raises(ValueError):
        Graph(_Cls())


def test_node_named():
    assert node(a, name="boop") == (a, "boop")


def test_create_graph():
    g = Graph(a)

    assert _node_def_equals(g.a, g, a, ("a",), (), {})


def test_create_graph_node_params():
    g = Graph(partial(a, 1, 2, named_arg="moof"))

    assert _node_def_equals(g.a, g, a, ("a",), (1, 2), {"named_arg": "moof"})


def test_create_named_node():
    g = Graph(node(a, "ping"))

    assert _node_def_equals(g.ping, g, a, ("ping",), (), {})


def test_set_node():
    g = Graph()

    # Simple assignment
    g.a = a
    assert _node_def_equals(g.a, g, a, ("a",), (), {})

    # Specific name
    g.moof = a
    assert _node_def_equals(g.moof, g, a, ("moof",), (), {})

    # Parametrized assignment
    g.b = partial(b, 1, 2, named_arg="moof")
    assert _node_def_equals(g.b, g, b, ("b",), (1, 2), {"named_arg": "moof"})


def test_override_node():
    g = Graph(a)

    assert _node_def_equals(g.a, g, a, ("a",), (), {})

    g.a = partial(b, 1, 2, named_arg="moof")

    assert _node_def_equals(g.a, g, b, ("a",), (1, 2), {"named_arg": "moof"})


def test_merged_graph():
    g1 = Graph(a)
    g2 = Graph(b)

    g = Graph(g1, g2)

    assert _node_def_equals(g.a, g, a, ("a",), (), {})
    assert _node_def_equals(g.b, g, b, ("b",), (), {})


def test_fail_add_graph_into_itself():
    g = Graph()

    with raises(ValueError):
        g.union(g)


def test_nested_subgraph():
    g1 = Graph(a)

    g = Graph(nested=g1)

    assert _node_def_equals(g.nested.a, g, a, ("nested", "a"), (), {})


def test_doubly_nested_subgraph():
    g1 = Graph(a)
    g2 = Graph(nested=g1)
    g = Graph(nested=g2)

    assert _node_def_equals(g.nested.nested.a, g, a, ("nested", "nested", "a"), (), {})


def test_set_subgraph():
    g = Graph()

    g.nested = Graph(a)
    g.nested.nested = Graph(b)

    assert _node_def_equals(g.nested.a, g, a, ("nested", "a"), (), {})
    assert _node_def_equals(g.nested.nested.b, g, b, ("nested", "nested", "b"), (), {})


def test_graph_pipe():
    g = Graph(a, b, c)

    g.nested = Graph(d)

    g.pipe(g.a, g.b, g.c.target_param, g.nested.d)

    assert g._downstream == {("a",): [EdgeDef(("b",), None)],
                             ("b",): [EdgeDef(("c",), "target_param")],
                             ("c",): [EdgeDef(("nested", "d"), None)],
                             ("nested", "d"): []}

    assert g._upstream == {("nested", "d"): [EdgeDef(("c",), None)],
                           ("c",): [EdgeDef(("b",), "target_param")],
                           ("b",): [EdgeDef(("a",), None)],
                           ("a",): []}


def test_graph_join():
    g = Graph(a, b, c)
    g.join([g.a, g.b], g.c.target_param)

    assert g._downstream == {("a",): [EdgeDef(("c",), "target_param")],
                             ("b",): [EdgeDef(("c",), "target_param")],
                             ("c",): []}

    assert g._upstream == {("c",): [EdgeDef(("a",), "target_param"), EdgeDef(("b",), "target_param")],
                           ("b",): [],
                           ("a",): []}


def test_graph_union():
    g1 = Graph(a)
    g1.nested = Graph(b)
    g1.pipe(g1.a, g1.nested.b)

    g = Graph(c)
    g.union(g1)

    assert _node_def_equals(g.a, g, a, ("a",), (), {})
    assert _node_def_equals(g.nested.b, g, b, ("nested", "b"), (), {})
    assert _node_def_equals(g.c, g, c, ("c",), (), {})

    assert g._downstream == {('a',): [EdgeDef(('nested', 'b'), None)],
                             ('nested', 'b'): [],
                             ('c',): []}
    assert g._upstream == {('a',): [],
                           ('nested', 'b'): [EdgeDef(('a',), None)],
                           ('c',): []}


def test_graph_named_union():
    g = Graph(a, b)
    g.union(nested=Graph(c))

    assert _node_def_equals(g.a, g, a, ("a",), (), {})
    assert _node_def_equals(g.b, g, b, ("b",), (), {})
    assert _node_def_equals(g.nested.c, g, c, ("nested", "c"), (), {})