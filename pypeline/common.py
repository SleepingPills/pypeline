from collections import namedtuple


EdgeDef = namedtuple("EdgeDef", "node param")


class NodeDef(object):
    """
    Node definition. Contains all the data relating to a node.
    """
    def __init__(self, owner, func, prefix, name, args=None, kwargs=None):
        self.owner = owner
        self.func = func
        self.name = name
        self.prefix = prefix
        self.path = prefix + (name,)
        self.args = args or ()
        self.kwargs = kwargs or {}

    def rebase(self, owner, prefix):
        """
        Rebase this node to another owner.

        :param owner: New owner.
        :param prefix: Prefix to rebase to.
        :return: Rebased node definition.
        """
        return NodeDef(owner, self.func, prefix, self.name, self.args, self.kwargs)

    def update(self, other):
        """
        Updates this node definition with the function and parameters of another.

        :param other: Other node definition to take data from.
        """
        self.func = other.func
        self.args = other.args
        self.kwargs = other.kwargs

    def __getattr__(self, key):
        return EdgeDef(self, key)