FROM ubuntu:22.04

RUN echo Version 3

ENV DEBIAN_FRONTEND=noninteractive
RUN apt update
RUN apt install -y python3
RUN apt install -y python3-dev
RUN apt install -y python3-pip
RUN apt install -y curl
RUN apt install -y git
RUN apt install -y build-essential
RUN apt install -y gfortran

RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && apt-get install google-cloud-cli -y

# Not needed by poltergust, but by many things we might want to run using poltergust

# commented out bc of M2 issues
RUN apt install -y libmkl-dev 
RUN apt install -y libgdal-dev

ADD . /app

WORKDIR /app

RUN pip install .

CMD ["/app/main.sh"]
