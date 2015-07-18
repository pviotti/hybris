#----------------------------------------------------------------------------
# DATE        : 2014, July 1st
# AUTHOR      : Paolo Viotti
# DESCRIPTION : Makefile for compiling source code and running unit tests
#----------------------------------------------------------------------------

SHELL := /bin/bash

# other useful plugins or Maven commands:
# * mvn site 
# * mvn dependency:analyze
# * maven-license-plugin
# * maven-rat-plugin
# * mvn javadoc:javadoc 

all: compile	

lib/libJerasure.jni.so:
	make -C jerasure

mavenize:
	mvn install:install-file -Dfile=lib/jerasure-jni-1.2.jar -DgroupId=eu.vandertil.jerasure -DartifactId=jerasure-jni -Dversion=1.2 -Dpackaging=jar
	mvn install:install-file -Dfile=lib/ycsb-core-0.1.4.jar -DgroupId=com.yahoo.ycsb -DartifactId=core -Dversion=0.1.4 -Dpackaging=jar

compile: mavenize lib/libJerasure.jni.so
	mvn package
	
clean:
	mvn clean
	make -C jerasure clean
	rm *.log
	
assembly:
	mvn clean package assembly:single

tree:
	mvn clean dependency:tree

test:
	mvn -DskipTests=false -Dtest=$(TEST) test

indent:
	find ./src/ -iname "*.java" -exec sed -i "s/\t/    /g" {} \;

loc:
	wc -l `find ./src/main -name "*.java"`
	
loctest:
	wc -l `find ./src/test -name "*.java"`

eclipse:
	mvn eclipse:eclipse
