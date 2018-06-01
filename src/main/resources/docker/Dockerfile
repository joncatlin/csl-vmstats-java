############################################################

# Set the base image to Java 1.8
FROM openjdk:8-jre-alpine

# File Author / Maintainer
MAINTAINER Jon Catlin

################## BEGIN INSTALLATION ######################
COPY /vmstats-IMAGE_VERSION.jar /home
COPY dependency /home/dependency

ENV COUCHBASE_NODES couchbase.mydomain.com
ENV STATSD_SERVER statsd.mydomain.com
ENV DIRNAME C:\Users\Temp
ENV FILETYPE .csv
ENV VMNAME_PATTERN [vV]{1}-.*

##################### INSTALLATION END #####################

# Set default container command
ENTRYPOINT java -classpath /home/vmstats-IMAGE_VERSION.jar:/home/dependency/* com.destini.vmstats.Start
