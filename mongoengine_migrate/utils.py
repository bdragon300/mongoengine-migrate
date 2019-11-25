class Slotinit(object):
    """
    Set class __slots__ in constructor kwargs
    If slot was omitted in kwargs then its value will be taken from
    `defaults` dict (if any). Other omitted slots will remain
    uninitialized.

    Example:
        class Car(Slotinit):
            __slots__ = ('color', 'engine_power', 'bodywork_type')
            defaults = {'color': 'black', 'bodywork_type': 'sedan'}

        # Slots will be:
        # color='blue', engine_power='100hp', bodywork_type='sedan'
        my_car = Car(color='blue', engine_power='100hp')
    """

    defaults = {}
    __slots__ = ()

    def __init__(self, **kwargs):
        for slot in self.__slots__:
            if slot in kwargs:
                setattr(self, slot, kwargs[slot])
            elif slot in self.defaults:
                setattr(self, slot, self.defaults[slot])

    def __eq__(self, other):
        if self is other:
            return True

        try:
            return all((
                    type(self) == type(other),
                    set(self.__slots__) == set(other.__slots__),
                    all(getattr(self, slot) == getattr(other, slot) for slot in self.__slots__)
            ))
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)
