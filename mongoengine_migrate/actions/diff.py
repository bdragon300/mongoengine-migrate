from typing import Any


class AlterDiff:
    # TODO: rename policy to error_policy
    # TODO: error_policy parameter:  'remove_field'
    policy_choices = ('ignore', 'modify', 'replace')
    default_policy = 'modify'

    def __init__(self,
                 old_value: Any,
                 new_value: Any,
                 policy: str = None,
                 default: Any = None):
        self.old = old_value
        self.new = new_value
        self.diff = (old_value, new_value)
        self.policy = policy if policy in self.policy_choices else self.default_policy
        self.default = default

    def swap(self):
        """Return swapped instance of the current one
        Swapped instance has changed new and old items from each other
        """
        return AlterDiff(self.new, self.old, self.policy, self.default)

    def to_python_expr(self) -> str:
        """Callback used in Action self-print method to get python
        string expression of this object"""
        expr = [repr(self.old), repr(self.new)]
        if self.policy != self.default_policy:
            expr.append(f'policy={self.policy!r}')

        if self.default is not None:
            expr.append(f'default={self.default!r}')

        return f"D({', '.join(expr)})"

    def __eq__(self, other):
        if self is other:
            return True

        return all((
            self.diff == other.diff,
            self.policy == other.policy,
            self.default == other.default
        ))

    def __ne__(self, other):
        return not self.__eq__(other)


# AlterDiff shortcut for using in migrations
D = AlterDiff
