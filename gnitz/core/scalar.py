from rpython.rlib import jit

class ScalarFunction(object):
    """
    Base class for logic executed inside MAP or FILTER.
    During translation, concrete implementations of this will be created.
    """
    _immutable_fields_ = []

    def evaluate_predicate(self, row_accessor):
        """Used by OP_FILTER. Returns True to keep row."""
        return True

    def evaluate_map(self, row_accessor, output_row_list):
        """Used by OP_MAP. Appends transformed TaggedValues to output_row_list."""
        pass
