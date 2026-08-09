FROM python:3.12-slim
WORKDIR /app/analytics

RUN mkdir -p /root/.dbt
COPY profiles.yml /root/.dbt/profiles.yml
ENV DBT_PROFILES_DIR=/root/.dbt

# aws cli, needed for dbt to work with athena backend
RUN apt-get update && \
    apt-get install -y curl unzip && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

# python requirements, dbt-core and dbt-athena-community
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r /app/requirements.txt

# dbt project itself (containing the dbt_project.yml)
COPY zavant_mlb_analytics .

CMD ["dbt", "run"]