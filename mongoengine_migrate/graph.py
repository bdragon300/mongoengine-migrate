__all__ = [
    'MigrationPolicy',
    'Migration',
    'MigrationsGraph'
]

from typing import Dict, List

from mongoengine_migrate.exceptions import MigrationGraphError
from mongoengine_migrate.utils import Slotinit
from enum import Enum


class MigrationPolicy(Enum):
    """Policy which determines how much the migration engine will
    be tolerant to already existing data which does not satisfy to
    mongoengine schema.

    MongoDB allows to keep any type of data in any field because of
    its schemaless nature. The data could be fed from various sources,
    not only those one with our mongoengine documents, MongoDB does not
    check their type.

    When `strict` policy is used the presence of data which does not
    satisfy to schema is treated as error and migration process will
    be interrupted.

    Using `relaxed` policy the migration engine does not check
    existing value types and tries to migrate data which related only to
    migration itself. E.g. when we change a field type from `long` to
    `string`, only `long` values will be casted to string type, but
    existing objectId or embedded documents or smth else in the same
    field will be skipped.
    """
    strict = 0
    relaxed = 1


class Migration(Slotinit):
    """Object represents one migration

    Contains information which is set in migration:
    * name -- migration file name without '.py' suffix
    * dependencies -- name list of migrations which this migration is
      dependent by
    * applied -- is migration was applied or not. Taken from database
    """
    __slots__ = ('name', 'dependencies', 'applied', 'module')
    defaults = {'applied': False}

    def get_actions(self):
        # FIXME: type checking, attribute checking
        # FIXME: tests
        return self.module.actions

    @property
    def policy(self) -> MigrationPolicy:
        attr = getattr(self.module, 'policy', MigrationPolicy.strict.name)
        return getattr(MigrationPolicy, attr)


class MigrationsGraph:
    # TODO: make it dict-like, not list-like
    def __init__(self):
        # Following two variables contains the same migrations digraph
        # but from different points of view
        self._parents: Dict[str, List[Migration]] = {}  # {child_name: [parent_obj...]}
        self._children: Dict[str, List[Migration]] = {}  # {parent_name: [child_obj...]}

        self._migrations: Dict[str, Migration] = {}  # {migration_name: migration_obj}

    @property
    def initial(self):
        """Return initial migration object"""
        for name, parents in self._parents.items():
            if not parents:
                return self._migrations[name]

    @property
    def last(self):
        """Return last children migration object"""
        for name, children in self._children.items():
            if not children:
                return self._migrations[name]

    @property
    def migrations(self):
        """Migration objects dict"""
        return self._migrations

    def add(self, migration: Migration):
        """
        Add migration to the graph. If object with that name exists
        in graph then it will be replaced.
        :param migration: Migration object
        :return:
        """
        self._parents[migration.name] = []
        self._children[migration.name] = []

        for partner in self._migrations.values():
            if partner.name == migration.name:
                continue
            if partner.name in migration.dependencies:
                self._parents[migration.name].append(partner)
                self._children[partner.name].append(migration)
            if migration.name in partner.dependencies:
                self._children[migration.name].append(partner)
                self._parents[partner.name].append(migration)

        self._migrations[migration.name] = migration

    def clear(self):  # TODO: tests
        """
        Clear graph
        :return:
        """
        self._parents = {}
        self._children = {}
        self._migrations = {}

    def verify(self):
        """
        Verify migrations graph to be satisfied to consistency rules
        Graph must not have loops, disconnections.
        Also it should have single initial migration and (for a while)
        single last migration.
        :raises MigrationGraphError: if problem in graph was found
        :return:
        """
        # FIXME: This function is not used anywhere
        initials = []
        last_children = []

        for name, obj in self._migrations.items():
            if not self._parents[name]:
                initials.append(name)
            if not self._children[name]:
                last_children.append(name)
            if len(obj.dependencies) > len(self._parents[name]):
                diff = set(obj.dependencies) - {x.name for x in self._parents[name]}
                raise MigrationGraphError(f'Unknown dependencies in migration {name!r}: {diff}')
            if name in (x.name for x in self._children[name]):
                raise MigrationGraphError(f'Found migration which dependent on itself: {name!r}')

        if len(initials) == len(last_children) and len(initials) > 1:
            raise MigrationGraphError(f'Migrations graph is disconnected, history segments '
                                 f'started on: {initials!r}, ended on: {last_children!r}')
        if len(initials) > 1:
            raise MigrationGraphError(f'Several initial migrations found: {initials!r}')

        if len(last_children) > 1:
            raise MigrationGraphError(f'Several last migrations found: {last_children!r}')

        if not initials or not last_children:
            raise MigrationGraphError(f'No initial or last children found')

    def walk_down(self, from_node: Migration, unapplied_only=True, _node_counters=None):
        """
        Walks down over migrations graph. Iterates in order as migrations
        should be applied.

        We're used modified DFS (depth-first search) algorithm to traverse
        the graph. Migrations are built into directed graph (digraph)
        counted from one root node to the last ones. Commonly DFS tries to
        walk to the maximum depth firstly.

        But there are some problems:
        * Graph can have directed cycles. This happens when some
          migration has several dependencies. Therefore we'll walk
          over such migration several times
        * Another problem arises from the first one. Typically we must walk
          over all dependencies before a dependent migration will be
          touched. DFS will process only one dependency before get to
          a dependent migration

        In order to manage it we use counter for each migration
        (node in digraph) initially equal to its parents count.
        Every time the algorithm gets to node it decrements this counter.
        If counter > 0 after that then don't touch this node and
        break traversing on this depth and go up. If counter == 0 then
        continue traversing.
        :param from_node: current node in graph
        :param unapplied_only: if True then return only unapplied migrations
         or return all migrations otherwise
        :param _node_counters:
        :raises MigrationGraphError: if graph has a closed cycle
        :return: Migration objects generator
        """
        # FIXME: may yield nodes not related to target migration if branchy graph
        #        if migration was applied after its dependencies unapplied then it is an error
        #        should have stable migrations order
        if _node_counters is None:
            _node_counters = {}
        if from_node is None:
            return ()
        _node_counters.setdefault(from_node.name, len(self._parents[from_node.name]) or 1)
        _node_counters[from_node.name] -= 1

        if _node_counters[from_node.name] > 0:
            # Stop on this depth if not all parents has been viewed
            return

        if _node_counters[from_node.name] < 0:
            # A node was already returned and we're reached it again
            # This means there is a closed cycle
            raise MigrationGraphError(f'Found closed cycle in migration graph, '
                                      f'{from_node.name!r} is repeated twice')

        if not (from_node.applied and unapplied_only):
            yield from_node

        for child in self._children[from_node.name]:
            yield from self.walk_down(child, unapplied_only, _node_counters)

    def walk_up(self, from_node: Migration, applied_only=True, _node_counters=None):
        """
        Walks up over migrations graph. Iterates in order as migrations
        should be reverted.

        We're using modified DFS (depth-first search) algorithm which in
        reversed order (see `walk_down`). Instead of looking at node
        parents count we're consider children count in order to return
        all dependent nodes before dependency.

        Because of the migrations graph may have many orphan child nodes
        they all should be passed as parameter
        :param from_node:  last children node we are starting for
        :param applied_only: if True then return only applied migrations,
         return all migrations otherwise
        :param _node_counters:
        :raises MigrationGraphError: if graph has a closed cycle
        :return: Migration objects generator
        """
        # FIXME: may yield nodes not related to reverting if branchy graph
        #        if migration was unapplied before its dependencies applied then it is an error
        if _node_counters is None:
            _node_counters = {}
        if from_node is None:
            return ()
        _node_counters.setdefault(from_node.name, len(self._children[from_node.name]) or 1)
        _node_counters[from_node.name] -= 1

        if _node_counters[from_node.name] > 0:
            # Stop in this depth if not all children has been viewed
            return

        if _node_counters[from_node.name] < 0:
            # A node was already returned and we're reached it again
            # This means there is a closed cycle
            raise MigrationGraphError(f'Found closed cycle in migration graph, '
                                      f'{from_node.name!r} is repeated twice')

        if from_node.applied or not applied_only:
            yield from_node

        for child in self._parents[from_node.name]:
            yield from self.walk_up(child, applied_only, _node_counters)

    def __iter__(self):
        return iter(self.walk_down(self.initial, unapplied_only=False))

    def __reversed__(self):
        return iter(self.walk_up(self.last, applied_only=False))

    def __contains__(self, migration: Migration):
        return migration in self._migrations.values()

    def __eq__(self, other):
        if other is self:
            return True
        if not isinstance(other, MigrationsGraph):
            return False

        return all(migr == other._migrations.get(name) for name, migr in self._migrations.items())

    def __ne__(self, other):
        return not self.__eq__(other)
