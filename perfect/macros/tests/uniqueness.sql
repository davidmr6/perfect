{% test uniqueness(model, column_name, partition_column=none, partition_start=none, partition_end=none) %}

with uniq as (
    select
        {{ column_name }},
        count(*) as cnt
    from {{ model }}
    {% if partition_column is not none %}
    where {{ partition_column }} between {{ var('start_date') }} and {{ var('end_date') }}
    {% endif %}
    group by {{ column_name }}
    having count(*) > 1
)

select *
from uniq

{% endtest %}