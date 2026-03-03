# gnitz/vm/query.py


class View(object):
    """
    A thin execution handle for a compiled view.
    Obtained from engine.get_view(view_id) or by wrapping an ExecutablePlan
    returned by ProgramCache.get_program().
    """

    _immutable_fields_ = ["view_id", "plan"]

    def __init__(self, view_id, plan):
        self.view_id = view_id
        self.plan = plan

    def process(self, delta_batch):
        """
        Processes a single batch of updates through the circuit.
        Returns a cloned output ZSetBatch if changes were produced, else None.
        """
        return self.plan.execute_epoch(delta_batch)
