from pypeline import in_out_dict


def test_in_out_dict():
    @in_out_dict
    def _transformer(val):
        return val ** 2

    assert _transformer(10) == 100

    assert _transformer({"a": 10, "b": 100}) == {"a": 100, "b": 10000}