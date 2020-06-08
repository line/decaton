FROM openjdk:8
MAINTAINER Decaton Developers

# Make profiler and taskstats cache
COPY ./debm.sh /decaton/debm.sh
RUN /decaton/debm.sh --profile --taskstats --dummy-option-to-fail || true

WORKDIR /decaton
CMD ./debm.sh
