FROM openjdk:17-alpine
## set working dir 
RUN mkdir /package
WORKDIR /package
## make mounted dirs
RUN mkdir output
RUN mkdir resources

## download latest logmap release 
RUN wget -O logmap.zip https://github.com/ernestojimenezruiz/logmap-matcher/releases/download/logmap-matcher-july-2021/logmap-matcher-standalone-july-2021.zip
RUN unzip logmap.zip 

