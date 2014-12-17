from functools import partial
from pypeline.context import params, group
from pypeline.graph import Graph, node, pipe


def a(x, y=5):
    return x * y


def b(value, fudge):
    return value + fudge


def c(val_a, val_b, fudge):
    return params(val_a + val_b + fudge, 1, 2, ping="pong")


def d(x1, x2, x3, ping=None, ping_override=None):
    return x1 + x2 + x3, ping_override or ping


def test_eval_node():
    g = Graph(node(lambda: 5, name="thunked_const"))()

    # Direct evaluation
    assert g.thunked_const() == 5
    # Cached evaluation
    assert g.thunked_const.val == 5


def test_eval_node_memoized():
    def gener():
        counter = 0
        while True:
            yield counter
            counter += 1

    g_inst = gener()

    g = Graph(node(lambda: g_inst.next(), name="accumulator"))()

    # Assert that direct eval does not cache results
    assert g.accumulator() == 0
    assert g.accumulator() == 1

    # Assert that the function is only ever called once and then memoized
    assert g.accumulator.val == 2
    assert g.accumulator.val == 2
    assert g.accumulator.val == 2


def test_eval_class_method():
    class _Cls(object):
        def __init__(self, y):
            self._y = y

        def plop(self, x):
            return x * self._y

    inst = _Cls(10)

    g = Graph(inst.plop)(plop=params(5))

    assert g.plop.val == 50


def test_eval_class_callable():
    class _Cls(object):
        def __init__(self, y):
            self._y = y

        def __call__(self, x):
            return x * self._y

    inst = _Cls(10)

    g = Graph(node(inst, "plop"))(plop=params(5))

    assert g.plop.val == 50


def test_eval_node_override_params():
    g = Graph(partial(a, 5, y=10))()

    # Assert that cached evaluation returns results with the default parameters
    assert g.a.val == 50

    # Direct evaluation
    assert g.a(5) == 25
    assert g.a(5, 6) == 30
    assert g.a(5, y=7) == 35

    # Assert that cached evaluation remains unchanged.
    assert g.a.val == 50


def test_node_set_params():
    g = Graph(partial(a, 5, y=10))()

    assert g.a.val == 50
    g.a.set(10, y=10)
    assert g.a.val == 100


def test_node_update_params():
    g = Graph(partial(a, 5, y=10))()

    # Assert that cached evaluation returns results with the default parameters
    assert g.a.val == 50

    # Update `y`
    g.a.update(y=7)

    assert g.a.val == 35


def test_node_set_params_invalidate():
    g = pipe(partial(a, 5), partial(b, fudge=10))()

    assert g.b.val == 35
    g.a.set(5, y=10)
    assert g.b.val == 60


def test_graph_set_target_params():
    nest_graph = Graph(a, b, sub=Graph(c, sub=Graph(d)))
    nest_graph.pipe(nest_graph.a, nest_graph.b)
    nest_graph.join([nest_graph.a, nest_graph.b], nest_graph.sub.c)
    nest_graph.pipe(nest_graph.sub.c, nest_graph.sub.sub.d)

    g = nest_graph(a=params(5, y=10), b=params(fudge=10), sub=group(c=params(fudge=20)))

    assert g.sub.sub.d.val == (133, "pong")
    assert g.sub.c.val == params(130, 1, 2, ping="pong")
    assert g.b.val == 60
    assert g.a.val == 50

    g.set(a=params(6, 10), b=params(fudge=20))

    assert g.sub.sub.d.val == (163, "pong")
    assert g.sub.c.val == params(160, 1, 2, ping="pong")
    assert g.b.val == 80
    assert g.a.val == 60

    g.sub.set(fudge=30, sub=group(d=params(ping_override="ping")))

    assert g.sub.sub.d.val == (173, "ping")
    assert g.sub.c.val == params(170, 1, 2, ping="pong")
    assert g.b.val == 80
    assert g.a.val == 60


def test_graph_set_global_params_kwargs():
    x_func = lambda data, fudge: data + fudge

    def y_func(data, fudge=20):
        return data + fudge

    # Create simple graph with a global param `fudge` set to 10
    # y_func is not an input node so the default value for `fudge` should not be affected there.
    g = pipe(node(x_func, "x"), node(y_func, "y"))(fudge=10, x=params(data=10))

    assert g.x.val == 20
    assert g.y.val == 30