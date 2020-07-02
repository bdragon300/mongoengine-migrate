from mongoengine_migrate.actions import *


dependencies = [
{%- if graph.last %}
    '{{ graph.last.name }}'
{%- endif %}
]

actions = [
{%- for action in actions_chain %}
    {{ action.to_python_expr() | symbol_wrap(96, wrapstring='\n        ') }},
{%- endfor %}
]
