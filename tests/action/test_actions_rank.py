import pytest

from mongoengine_migrate import actions


@pytest.fixture
def actions_rank():
    """Actions are sorted by priority ascending"""
    return [
        actions.RenameEmbedded,
        actions.CreateEmbedded,
        actions.AlterEmbedded,
        actions.RenameDocument,
        actions.CreateDocument,
        actions.AlterDocument,
        actions.RenameField,

        # Has the default priority
        actions.AlterField,
        actions.CreateField,
        actions.DropField,
        actions.RunPython,

        actions.AlterIndex,
        actions.DropIndex,
        actions.CreateIndex,
        actions.DropDocument,
        actions.DropEmbedded
    ]


def test_actions_rank(actions_rank):
    registry_actions = sorted([(c.priority, c.__name__) for c in actions.actions_registry.values()])
    ranked_actions = sorted([(c.priority, c.__name__) for c in actions_rank])

    assert registry_actions == ranked_actions
