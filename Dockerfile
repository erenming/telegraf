FROM registry.erda.cloud/retag/golang:1.16 as build

RUN apt-get update && apt-get -y install libpcap-dev

COPY . /root/build
WORKDIR /root/build

ENV GOPROXY="https://goproxy.cn"
RUN set -ex && echo "Asia/Shanghai" > /etc/timezone

RUN make telegraf

FROM registry.cn-hangzhou.aliyuncs.com/terminus/terminus-centos:base

RUN mkdir -p /app/conf

WORKDIR /app

RUN echo "Asia/Shanghai" >/etc/timezone

RUN yum -y install sysstat
RUN yum -y install ntp
RUN yum -y install libpcap libpcap-devel

COPY --from=build /root/build/telegraf /app/
COPY --from=build /root/build/conf /app/conf
COPY --from=build /root/build/exec_scripts /app/exec_scripts
COPY --from=build /root/build/entrypoint.sh /app/

CMD ["./entrypoint.sh"]
