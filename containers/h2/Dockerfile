FROM alpine/java:21-jdk

ARG H2_VERSION
RUN mkdir /data /h2 \
    && chown 1000 /data
RUN wget "https://github.com/h2database/h2database/releases/download/version-${H2_VERSION}/h2-${H2_VERSION}.jar" -O "/h2/h2-${H2_VERSION}.jar"  \
    && ln -s "/h2/h2-${H2_VERSION}.jar" /h2/h2.jar

USER 1000
ENV HOME=/data
WORKDIR /tmp

CMD ["java", \
     "-Duser.home=/data", \
     "-cp", "/h2/h2.jar", "org.h2.tools.Server", \
     "-web", "-webAllowOthers", \
     "-tcp", "-tcpAllowOthers", \
     "-baseDir", "/data", "-ifNotExists" \
     ]
