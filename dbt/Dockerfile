FROM python:3.13

LABEL maintainer="kennys_transjakarta" \
      description="Docker image for Transjakarta dataset end-to-end project"


WORKDIR /app


RUN apt-get update && apt-get install -qq -y \
    git gcc build-essential  --fix-missing --no-install-recommends \
    && apt-get clean

RUN pip install --upgrade pip


RUN mkdir -p /root/.dbt

#
COPY requirements.txt requirements.txt


RUN pip install -r requirements.txt 

COPY profiles.template.yml /root/.dbt/profiles.yml


COPY . /app

CMD ["dbt","build"]