FROM python:3.13

LABEL maintainer="kennys_transjakarta" \
      description="Docker image for Transjakarta dataset end-to-end project"

#set working directory
WORKDIR /app

# Install os dependencies
RUN apt-get update && apt-get install -qq -y \
    git gcc build-essential  --fix-missing --no-install-recommends \
    && apt-get clean
# makesure we use the up to date pip
RUN pip install --upgrade pip

# Create directory for dbt config
RUN mkdir -p /root/.dbt

#Copy requirement.txt
COPY requirements.txt requirements.txt

# install depedencies
RUN pip install -r requirements.txt 

#copy profile.yml
COPY profiles.yml /root/.dbt/profiles.yml

#copy source code
COPY . /app

#start the rmc server
CMD ["dbt","build"]