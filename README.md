Hybris
======

Hybris is the prototype of a Java server side library which implements the 
hybrid cloud based data storage described in [[1\]][1].  

Hybris code base is composed by two main modules: MdStore and KvStore, 
the first being a thin wrapper layer of the metadata storage engine (i.e. Zookeeper),
while the latter implements the storage primitives towards the API of the 
public cloud storage services.  

In this context the Hybris server should expose a simple set of APIs 
and coordinate the two modules in order to achieve these general goals:

 * data availability
 * data integrity
 * byzantine fault tolerance
 * avoid cloud storage lock-in

Notice that for now other security aspects such as authentication and data confidentiality
are left apart, since the first and very purpose of Hybris is implementing
and testing a cross- cloud providers storage system which is designed to
ensure byzantine fault tolerance at a relative low cost.  

Another goal is to make this library somehow compatible with the StackSync prototype.


Technical requirements
----------------------

 * Java 1.6+
 * Zookeeper 3.4.5
 * jClouds

Development tools:

 * Maven
 * Git
 * Eclipse (with Egit and m2e plugins)


TODO
----

 * data deduplication -> check student project (?)
 * cloud latency check and optimization -> check student project (?)
 * metadata layout on Zookeeper
     * `` / MdStore / $HybrisUserId / $fileId / $chunkId ``
     * `` / MdStore / $HybrisUserId / $fileId / $chunkId / $old_ver ``
     * data stored on ZK: replicas reference list, timestamp, hash value
 * metadata serialization
     * consider not to use default Java serialization: downsides about security and compatibility (among programming languages as well as different versions of serialized classes; [ref1](http://en.wikipedia.org/wiki/Java_serialization#Java), [ref2](http://uet.vnu.edu.vn/~chauttm/e-books/java/Effective.Java.2nd.Edition.May.2008.3000th.Release.pdf) pag. 312)
     * other candidates:
         * [protobuf](http://code.google.com/p/protobuf/) by Google


 [1]: http://arxiv.org/abs/1305.4868        "BFT Storage with 2t+1 Data Replicas - C. Cachin, D. Dobre, M. Vukolic"
