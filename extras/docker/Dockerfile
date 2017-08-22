FROM alpine:3.2
MAINTAINER ola.angelsmark@ericsson.com
ARG branch=master
#proxy settings
ENV http_proxy=http://proxy.rd.francetelecom.fr:8080
ENV FTP_PROXY=http://proxy.rd.francetelecom.fr:8080
ENV ftp_proxy=http://proxy.rd.francetelecom.fr:8080
ENV https_proxy=http://proxy.rd.francetelecom.fr:8080
ENV HTTPS_PROXY=http://proxy.rd.francetelecom.fr:8080
ENV no_proxy=localhost,125.0.0.1,10.193.0.0,10.194.0.0,.francetelecom.fr,192.168.0.100,127.0.0.1
ENV HTTP_PROXY=http://proxy.rd.francetelecom.fr:8080
RUN apk --update add python py-pip openssl ca-certificates \
      && apk --update add --virtual build-dependencies build-base git gcc python-dev libffi-dev openssl-dev \
      && pip install --upgrade pip \
      && git clone -b $branch https://github.com/EricssonResearch/calvin-base calvin-base \
      && cd /calvin-base \
      && pip install --upgrade -r requirements.txt -r test-requirements.txt -e . \
      && apk del build-dependencies && rm -rf /var/cache/apk/*
RUN apk --update add curl
#proxy settings
ENV http_proxy ""
ENV FTP_PROXY ""
ENV ftp_proxy "" 
ENV https_proxy ""
ENV HTTPS_PROXY ""
ENV no_proxy ""
ENV HTTP_PROXY ""

WORKDIR /calvin-base/
EXPOSE 5000 5001

