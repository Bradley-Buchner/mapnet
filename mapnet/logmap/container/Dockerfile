FROM openjdk:17-alpine
RUN mkdir /package
WORKDIR /package
COPY mapnet/logmap/logmap ./logmap
COPY mapnet/logmap/logmap/parameters.txt ./parameters.txt
## These two lines need to be modfied in the docker run ##
# COPY resources ./resources
# COPY mapnet/logmap-matcher/run.sh ./
#########################################################
RUN mkdir output
RUN mkdir resources

