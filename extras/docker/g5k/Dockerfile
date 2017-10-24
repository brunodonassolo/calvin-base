FROM alpine:3.2
ARG branch=networkMonitor
ARG port=5000
ARG control_port=5001
ARG netdata_port=19999
ARG blackbox_port=9115

#calvin
RUN apk --update add python py-pip openssl ca-certificates \
      && apk --update add --virtual build-dependencies build-base git gcc python-dev libffi-dev openssl-dev \
      && pip install --upgrade pip \
      && git clone -b $branch https://github.com/brunodonassolo/calvin-base.git calvin-base \
      && cd /calvin-base \
      && pip install --upgrade -r requirements.txt -r test-requirements.txt -e . \
      && apk del build-dependencies && rm -rf /var/cache/apk/*
RUN apk --update add curl

#netdata and fireqos
RUN apk add bash ipset iproute2
RUN bash -c "bash <(curl -Ss https://my-netdata.io/kickstart.sh) --dont-wait"
RUN bash -c "bash <(curl -Ss https://raw.githubusercontent.com/firehol/netdata-demo-site/master/install-all-firehol.sh) --dont-wait"

#blackbox_exporter
RUN curl -Ss https://storage.googleapis.com/golang/go1.9.1.linux-amd64.tar.gz --output go1.9.1.linux-amd64.tar.gz
RUN tar -C /usr/local -xzf go1.9.1.linux-amd64.tar.gz
ENV PATH=$PATH:/usr/local/go/bin
RUN mkdir -p $HOME/go
RUN mkdir -p /lib64 && ln -s /lib/libc.musl-x86_64.so.1 /lib64/ld-linux-x86-64.so.2
RUN go get github.com/prometheus/blackbox_exporter
RUN cd $HOME/go/src/github.com/prometheus/blackbox_exporter && make build
ENV PATH=$PATH:/root/go/bin/

WORKDIR /calvin-base/
EXPOSE $port $control_port $netdata_port $blackbox_port

COPY calvin_startup.sh calvin_startup.sh

ENV CALVIN_PORT=$port
ENV CALVIN_CONTROL_PORT=$control_port
CMD ./calvin_startup.sh
