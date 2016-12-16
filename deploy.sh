#!/bin/sh

# To use this you must:
# specify the repositoryID and username/password in your ~/.m2/settings.xml file:
# <servers> <server> <id>internal-maven-repository</id> <username>maven</username> </server> </servers>
# and setup passwordless ssh authentication

# example: ./deploy.sh 1.2.8

SERVER="scp://ccms@ccms-support.ucsd.edu:/var/www/html/maven2"
URL="http://ccms-support.ucsd.edu/maven2"

echo deploying to $URL 

deploy()
{
	mvn deploy:deploy-file \
	-DgroupId=$1 \
	-DartifactId=$2 \
	-Dversion=$3 \
	-Dpackaging=jar \
	-Dfile=$4\
	-DrepositoryId=internal-maven-repository \
	-Durl=$SERVER
	# add "pomFile=<filename>" to deploy plugin if you've got a pom 
}

deploy_source()
{
	mvn deploy:deploy-file \
	-DgroupId=$1 \
	-DartifactId=$2 \
	-Dversion=$3 \
	-Dpackaging=java-source \
	-Dfile=$4\
	-DrepositoryId=internal-maven-repository \
	-Durl=$SERVER
	# add "pomFile=<filename>" to deploy plugin if you've got a pom 
}

mvn clean assembly:assembly
deploy edu.ucsd.mztab MzTabUtils $1 target/MzTabUtils.jar 
deploy_source edu.ucsd.mztab MzTabUtils $1 target/MzTabUtils-sources.jar
