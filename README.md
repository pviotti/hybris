Hybris
======

Hybris is the prototype of a Java server side library which implements the 
hybrid cloud based data storage described in [[1\]][1].  

Hybris code base is composed by two main modules: MdStore and KvStore, 
the first being a thin wrapper layer of the metadata storage engine (i.e. Zookeeper),
while the other implements the storage primitives towards the API of the 
public cloud storage services.  

In this context the Hybris server should expose a simple set of APIs 
and coordinate the two modules in order to achieve these general goals:

 * data availability
 * data integrity
 * byzantine fault tolerance
 * avoid vendor lock-in

Notice that other security aspects such as authentication and data confidentiality
are out of scope, since the first and very purpose of Hybris is implementing
and testing a cross- cloud providers storage system which is designed to
ensure byzantine fault tolerance at a relatively low cost.  

Another goal is to make this library somehow compatible with the StackSync prototype.


Technical requirements
----------------------

 * Java 1.6+
 * Zookeeper 3.4.5
 * jClouds 1.6

For more information see the [development info wiki page][1].

 [1]: https://bitbucket.org/pviotti/hybris/wiki/Development%20info      "Development info"
