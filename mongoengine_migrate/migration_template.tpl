{{ import_expressions | sort | join("\n") }}

# Existing data processing policy
# Possible values are: {{ policy_enum | map(attribute="name") | join(", ") }}
policy = "strict"

# Names of migrations which the current one is dependent by
dependencies = [
{%- if graph.last %}
    '{{ graph.last.name }}'
{%- endif %}
]

# Action chain
actions = [
{%- for action in actions_chain %}
    {{ action.to_python_expr() | symbol_wrap(96, wrapstring='\n        ') }},
{%- endfor %}
]
