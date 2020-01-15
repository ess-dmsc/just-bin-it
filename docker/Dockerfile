FROM ubuntu:bionic
ENV PARAMETERS='NO PARAMETERS SUPPLIED'

RUN apt update && \
    apt install -yq wget build-essential git software-properties-common curl

RUN add-apt-repository -y ppa:deadsnakes/ppa

RUN apt install -yq python3.7

RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && python3.7 get-pip.py

RUN git clone https://github.com/ess-dmsc/just-bin-it.git && \
    cd just-bin-it && \
    python3.7 -m pip install -r requirements.txt

WORKDIR just-bin-it

CMD python3.7 bin/just-bin-it.py $PARAMETERS
