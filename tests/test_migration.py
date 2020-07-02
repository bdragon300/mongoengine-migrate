import random

import pytest

from mongoengine_migrate.migration import Migration, MigrationsGraph
from mongoengine_migrate.exceptions import MongoengineMigrateError


@pytest.fixture
def migration_tree():
    """
      _____(01)_____
     V      V       V
    (02)   (03)    (04)__
     |      |      V     V
      \     /    (05)  (06)
       \   /_____/  \  /
        V VV         VV
        (07)        (08)
           \___  ___/
               VV
              (09)
               V
              (10)
    """
    return [
        Migration(name='01', dependencies=[]),
        Migration(name='02', dependencies=['01']),
        Migration(name='03', dependencies=['01']),
        Migration(name='04', dependencies=['01']),
        Migration(name='05', dependencies=['04']),
        Migration(name='06', dependencies=['04']),
        Migration(name='07', dependencies=['02', '03', '05']),
        Migration(name='08', dependencies=['05', '06']),
        Migration(name='09', dependencies=['07', '08']),
        Migration(name='10', dependencies=['09']),
    ]


class TestMigrationGraph:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.obj = MigrationsGraph()

    def test_initial_prop__should_return_initial_migration_object(self, migration_tree):
        for m in migration_tree:
            self.obj.add(m)

        res = self.obj.initial

        assert res == migration_tree[0]

    def test_initial_prop__on_empty_graph__should_return_none(self):
        res = self.obj.initial

        assert res is None

    def test_last_prop__should_return_last_migration(self, migration_tree):
        for m in migration_tree:
            self.obj.add(m)

        res = self.obj.last

        assert res == migration_tree[-1]

    def test_last_prop__on_empty_graph__should_return_none(self):
        res = self.obj.last

        assert res is None

    def test_add__on_migration_order_is_shuffled__should_fill_parents_correctly(self,
                                                                                migration_tree):
        random.shuffle(migration_tree)
        migrations = {m.name: m for m in migration_tree}
        expect_parents = {
            '01': [],
            '02': [migrations['01']],
            '03': [migrations['01']],
            '04': [migrations['01']],
            '05': [migrations['04']],
            '06': [migrations['04']],
            '07': [migrations['02'], migrations['03'], migrations['05']],
            '08': [migrations['05'], migrations['06']],
            '09': [migrations['07'], migrations['08']],
            '10': [migrations['09']]
        }

        for m in migration_tree:
            self.obj.add(m)

        assert self.obj._parents.keys() == expect_parents.keys()
        assert all(e in self.obj._parents[k]
                   for k in expect_parents.keys()
                   for e in expect_parents[k])

    def test_add__on_migration_order_is_shuffled__should_fill_children_correctly(self,
                                                                                 migration_tree):
        random.shuffle(migration_tree)
        migrations = {m.name: m for m in migration_tree}
        expect_children = {
            '01': [migrations['02'], migrations['03'], migrations['04']],
            '02': [migrations['07']],
            '03': [migrations['07']],
            '04': [migrations['05'], migrations['06']],
            '05': [migrations['07'], migrations['08']],
            '06': [migrations['08']],
            '07': [migrations['09']],
            '08': [migrations['09']],
            '09': [migrations['10']],
            '10': []
        }

        for m in migration_tree:
            self.obj.add(m)

        assert self.obj._children.keys() == expect_children.keys()
        assert all(e in self.obj._children[k]
                   for k in expect_children.keys()
                   for e in expect_children[k])

    def test_add__on_migration_order_is_shuffled__should_fill_migrations_correctly(self,
                                                                                   migration_tree):
        random.shuffle(migration_tree)
        expect_migrations = {m.name: m for m in migration_tree}

        for m in migration_tree:
            self.obj.add(m)

        assert self.obj.migrations.items() == expect_migrations.items()

    def test_add__if_migration_already_in_graph__should_replace_migration_object(self,
                                                                                 migration_tree):
        random.shuffle(migration_tree)
        another_migration = Migration(name='05', dependencies=['04', '03'], applied=True)

        for m in migration_tree:
            self.obj.add(m)
        self.obj.add(another_migration)

        assert self.obj.migrations['05'] is another_migration

    def test_add__if_migration_already_in_graph__should_rebuild_parents_links(self, migration_tree):
        random.shuffle(migration_tree)
        migrations = {m.name: m for m in migration_tree}
        expect_parents = {
            '01': [],
            '02': [migrations['01']],
            '03': [migrations['01']],
            '04': [migrations['01']],
            '05': [migrations['04'], migrations['03']],
            '06': [migrations['04']],
            '07': [migrations['02'], migrations['03'], migrations['05']],
            '08': [migrations['05'], migrations['06']],
            '09': [migrations['07'], migrations['08']],
            '10': [migrations['09']]
        }
        another_migration = Migration(name='05', dependencies=['04', '03'], applied=True)

        for m in migration_tree:
            self.obj.add(m)
        self.obj.add(another_migration)

        assert self.obj._parents.keys() == expect_parents.keys()
        assert all(e in self.obj._parents[k]
                   for k in expect_parents.keys()
                   for e in expect_parents[k])

    def test_add__if_migration_already_in_graph__should_rebuild_children_links(self, migration_tree):
        random.shuffle(migration_tree)
        migrations = {m.name: m for m in migration_tree}
        expect_children = {
            '01': [migrations['02'], migrations['03'], migrations['04']],
            '02': [migrations['07']],
            '03': [migrations['07']],
            '04': [migrations['05'], migrations['06']],
            '05': [migrations['07'], migrations['08']],
            '06': [migrations['08']],
            '07': [migrations['09']],
            '08': [migrations['09']],
            '09': [migrations['10']],
            '10': []
        }
        another_migration = Migration(name='05', dependencies=['04', '03'], applied=True)

        for m in migration_tree:
            self.obj.add(m)
        self.obj.add(another_migration)

        assert self.obj._children.keys() == expect_children.keys()
        assert all(e in self.obj._children[k]
                   for k in expect_children.keys()
                   for e in expect_children[k])

    def test_verify__if_graph_is_correct__should_return_nothing(self, migration_tree):
        for m in migration_tree:
            self.obj.add(m)

        res = self.obj.verify()

        assert res is None

    def test_verify__if_graph_is_empty__should_return_nothing(self):
        res = self.obj.verify()

        assert res is None

    def test_verify__if_wrong_dependency__should_raise_migration_error(self, migration_tree):
        migration_tree[4] = Migration(name='05', dependencies=['wrong_dep'])
        for m in migration_tree:
            self.obj.add(m)

        with pytest.raises(MongoengineMigrateError) as e:
            self.obj.verify()

        assert 'Unknown dependencies' in str(e)
        assert '05' in str(e)

    def test_verify__if_migration_is_dependent_on_itself__should_raise_migration_error(self,
                                                                                       migration_tree):
        migration_tree[4] = Migration(name='05', dependencies=['05'])
        for m in migration_tree:
            self.obj.add(m)

        with pytest.raises(MongoengineMigrateError) as e:
            self.obj.verify()

        assert 'dependent on itself' in str(e)
        assert '05' in str(e)

    def test_verify__if_graph_has_disconnects__should_raise_migration_error(self, migration_tree):
        migration_tree[8] = Migration(name='09', dependencies=[])
        for m in migration_tree:
            self.obj.add(m)

        with pytest.raises(MongoengineMigrateError) as e:
            self.obj.verify()

        assert 'Migrations graph is disconnected' in str(e)
        assert '09' in str(e)

    def test_verify__if_graph_has_several_initial_migrations__should_raise_migration_error(
            self,
            migration_tree
    ):
        migration_tree[4] = Migration(name='05', dependencies=[])
        for m in migration_tree:
            self.obj.add(m)

        with pytest.raises(MongoengineMigrateError) as e:
            self.obj.verify()

        assert 'Several initial migrations' in str(e)
        assert '05' in str(e)

    def test_verify__if_graph_has_several_last_migrations__should_raise_migration_error(
            self,
            migration_tree
    ):
        migration_tree.append(Migration(name='11', dependencies=['09']))
        for m in migration_tree:
            self.obj.add(m)

        with pytest.raises(MongoengineMigrateError) as e:
            self.obj.verify()

        assert 'Several last migrations' in str(e)
        assert '11' in str(e)

    def test_verify__if_graph_has_no_initial_or_last_migration__should_raise_migration_error(
            self,
            migration_tree
    ):
        migration_tree[0] = Migration(name='01', dependencies=['10'])
        for m in migration_tree:
            self.obj.add(m)

        with pytest.raises(MongoengineMigrateError) as e:
            self.obj.verify()

        assert 'No initial or last children' in str(e)

    @pytest.mark.parametrize('from_node,expect_order', (
        ('01', ['01', '02', '03', '04', '05', '07', '06', '08', '09', '10']),
        ('05', ['05']),
        ('04', ['04', '05', '06']),
        ('09', ['09', '10']),
        ('10', ['10'])
    ))
    def test_walk_down__should_walk_from_start_node_considering_dependencies(
            self,
            migration_tree,
            from_migration,
            expect_order
    ):
        for m in migration_tree:
            self.obj.add(m)
        migrations = {m.name: m for m in migration_tree}

        res = self.obj.walk_down(migrations[from_migration])

        assert [m.name for m in res] == expect_order

    def test_walk_down__if_some_migrations_were_already_applied__should_not_return_them_by_default(
            self,
            migration_tree
    ):
        migrations = {m.name: m for m in migration_tree}
        for i in ('01', '02', '03', '04', '08'):
            migrations[i].applied = True
        for m in migrations.values():
            self.obj.add(m)
        expect_order = ['05', '07', '06', '09', '10']

        res = self.obj.walk_down(self.obj.initial)

        assert [m.name for m in res] == expect_order

    def test_walk_down__if_some_migrations_were_applied_and_forced_to_return__should_return_all(
            self,
            migration_tree
    ):
        migrations = {m.name: m for m in migration_tree}
        for i in ('01', '02', '03', '04', '08'):
            migrations[i].applied = True
        for m in migrations.values():
            self.obj.add(m)
        expect_order = ['01', '02', '03', '04', '05', '07', '06', '08', '09', '10']

        res = self.obj.walk_down(self.obj.initial, unapplied_only=False)

        assert [m.name for m in res] == expect_order

    def test_walk_down__if_no_migrations_added__should_return_empty_generator(self):
        res = self.obj.walk_down(self.obj.initial)

        assert list(res) == []

    @pytest.mark.parametrize('from_node,expect_order', (
        ('10', ['10', '09', '07', '02', '03', '08', '05', '06', '04', '01']),
        ('08', ['08', '06']),
        ('07', ['07', '02', '03']),
        ('03', ['03']),
        ('01', ['01'])
    ))
    def test_walk_up__should_walk_from_start_node_considering_dependencies(
            self,
            migration_tree,
            from_migration,
            expect_order
    ):
        for m in migration_tree:
            self.obj.add(m)
        migrations = {m.name: m for m in migration_tree}

        res = self.obj.walk_up(migrations[from_migration])

        assert [m.name for m in res] == expect_order

    def test_walk_up__if_some_migrations_were_already_unapplied__should_not_return_them_by_default(
            self,
            migration_tree
    ):
        migrations = {m.name: m for m in migration_tree}
        for i in ('01', '02', '03', '04', '09'):
            migrations[i].applied = True
        for m in migrations.values():
            self.obj.add(m)
        expect_order = ['10', '07', '02', '03', '08', '05', '06']

        res = self.obj.walk_up(self.obj.last)

        assert [m.name for m in res] == expect_order

    def test_walk_up__if_some_migrations_were_unapplied_and_forced_to_return__should_return_all(
            self,
            migration_tree
    ):
        migrations = {m.name: m for m in migration_tree}
        for i in ('01', '02', '03', '04', '09'):
            migrations[i].applied = True
        for m in migrations.values():
            self.obj.add(m)
        expect_order = ['10', '09', '07', '02', '03', '08', '05', '06', '04', '01']

        res = self.obj.walk_up(self.obj.last, applied_only=False)

        assert [m.name for m in res] == expect_order

    def test_walk_up__if_no_migrations_added__should_return_empty_generator(self):
        res = self.obj.walk_up(self.obj.last)

        assert list(res) == []

    def test_iter__should_walk_down_from_the_initial_node(self, migration_tree):
        for m in migration_tree:
            self.obj.add(m)
        expect_order = ['01', '02', '03', '04', '05', '07', '06', '08', '09', '10']

        assert list(x.name for x in self.obj) == expect_order

    def test_reversed__should_walk_up_from_the_last_node(self, migration_tree):
        for m in migration_tree:
            self.obj.add(m)
        expect_order = ['10', '09', '07', '02', '03', '08', '05', '06', '04', '01']

        assert list(x.name for x in reversed(self.obj)) == expect_order

    def test_contains__if_migration_is_contained_in_graph__should_return_true(self,
                                                                              migration_tree):
        for m in migration_tree:
            self.obj.add(m)

        assert migration_tree[3] in self.obj

    def test_contains__if_migration_is_not_contained_in_graph__should_return_false(self,
                                                                                   migration_tree):
        for m in migration_tree[:3]:
            self.obj.add(m)

        assert migration_tree[7] not in self.obj

    def test_eq_ne__migrations_are_equal__graphs_should_be_equal(self, migration_tree):
        obj = MigrationsGraph()
        for m in migration_tree:
            self.obj.add(m)
            obj.add(m)

        assert self.obj == obj
        assert not self.obj != obj

    def test_eq_ne__if_two_migrations_in_graphs_are_differ__graphs_should_be_not_equal(
            self,
            migration_tree
    ):
        obj = MigrationsGraph()
        test_migration05 = Migration(name="test_name", dependencies=['04'])
        for m in migration_tree:
            self.obj.add(m)
            obj.add(m)
        obj.migrations['05'] = test_migration05

        assert not self.obj == obj
        assert not obj == self.obj
        assert self.obj != obj
        assert obj != self.obj

    def test_eq_ne__if_extra_migration_in_second_graph__graphs_should_be_not_equal(self,
                                                                                   migration_tree):
        obj = MigrationsGraph()
        test_migration05 = Migration(name="test_name", dependencies=['04'])
        for m in migration_tree:
            self.obj.add(m)
            obj.add(m)
        obj.add(test_migration05)

        assert not self.obj == obj
        assert not obj == self.obj
        assert self.obj != obj
        assert obj != self.obj

    def test_eq_ne__if_second_graph_is_empty__graphs_should_be_not_equal(self, migration_tree):
        obj = MigrationsGraph()
        for m in migration_tree:
            self.obj.add(m)

        assert not self.obj == obj
        assert not obj == self.obj
        assert self.obj != obj
        assert obj != self.obj
