import pytest
from mongoengine_migrate.utils import Slotinit


class SlotinitStub(Slotinit):
    __slots__ = ('slot1', 'slot2', 'slot3', 'slot4')
    defaults = {'slot2': 'default_value2', 'slot3': 'default_value3'}


class SlotinitDerivedStub(SlotinitStub):
    pass


class SlotinitStub2(Slotinit):
    __slots__ = ('slot1', 'slot2', 'slot3', 'slot4')
    defaults = {'slot2': 'default_value2', 'slot3': 'default_value3'}


class SlotinitStubAnotherSlots(Slotinit):
    __slots__ = ('slot1', 'slot2', 'slot4')
    defaults = {'slot2': 'default_value2'}


class SlotinitStubSlotsInAnotherOrder(Slotinit):
    __slots__ = ('slot4', 'slot2', 'slot3', 'slot1')
    defaults = {'slot2': 'default_value2', 'slot3': 'default_value3'}


class TestSlotinit:
    def test_init__should_initizlize_slots(self):
        slot_data = {
            'slot1': '1',
            'slot3': 'not default value',
            'slot4': 678
        }
        expect = {
            'slot1': '1',
            'slot2': 'default_value2',
            'slot3': 'not default value',
            'slot4': 678
        }

        obj = SlotinitStub(**slot_data)

        assert {s: getattr(obj, s) for s in obj.__slots__} == expect

    def test_init__missed_slots_should_not_be_initilized(self):
        obj = SlotinitStub()

        with pytest.raises(AttributeError):
            assert obj.slot1

    def test_eq_ne__on_equal_slot_values__should_be_equal(self):
        obj1 = SlotinitStub(slot1=1, slot4=4, slot3='default_value3')
        # slot3 has the same default value
        obj2 = SlotinitStub(slot1=1, slot4=4)

        assert obj1 == obj2
        assert not obj1 != obj2

    def test_eq_ne__when_equal_slots_in_different_order_and_the_same_values__should_be_equal(self):
        obj1 = SlotinitStub(slot1=1, slot4=4)
        obj2 = SlotinitStubSlotsInAnotherOrder(slot1=1, slot4=4)

        assert obj1 == obj2
        assert not obj1 != obj2

    @pytest.mark.parametrize('initial_data', (
        {'slot1': 123, 'slot4': 456},
        {'slot1': 123}  # not all slots was initialized
    ))
    def test_eq_ne__on_the_same_object__should_always_be_equal(self, initial_data):
        obj1 = SlotinitStub(**initial_data)

        assert obj1 == obj1
        assert not obj1 != obj1

    @pytest.mark.parametrize('obj_type', (SlotinitDerivedStub, SlotinitStub2))
    def test_eq_ne__on_different_types__should_not_be_equal(self, obj_type):
        obj1 = SlotinitStub(slot1=1, slot4=4)
        obj2 = obj_type(slot1=1, slot4=4)

        assert not obj1 == obj2
        assert obj1 != obj2

    def test_eq_ne__on_different_slots__should_not_be_equal(self):
        obj1 = SlotinitStub(slot1=1, slot4=4)
        obj2 = SlotinitStubAnotherSlots(slot1=1, slot4=4)

        assert not obj1 == obj2
        assert obj1 != obj2

    @pytest.mark.parametrize('initial_data1, initial_data2', (
        ({'slot1': 1, 'slot4': 4}, {'slot1': 1, 'slot4': 123}),
        ({'slot1': 1, 'slot4': 4}, {'slot1': 1, 'slot4': 4, 'slot3': 'not default value'}),
    ))
    def test_eq_ne__on_different_values__should_not_be_equal(self, initial_data1, initial_data2):
        obj1 = SlotinitStub(**initial_data1)
        obj2 = SlotinitStub(**initial_data2)

        assert not obj1 == obj2
        assert obj1 != obj2

    def test_eq_ne__if_some_slots_was_not_initialized__should_not_be_equal(self):
        obj1 = SlotinitStub(slot1=1, slot4=4)
        obj2 = SlotinitStub(slot1=1)

        assert not obj1 == obj2
        assert not obj2 == obj1
        assert obj1 != obj2
        assert obj2 != obj1
