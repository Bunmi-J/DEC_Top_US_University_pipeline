FROM quay.io/astronomer/astro-runtime:12.7.1

COPY require.txt /tmp
RUN python -m pip install -r /tmp/require.txt

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-core dbt-snowflake && deactivate