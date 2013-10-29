Hybris
======

Hybris is the prototype of a Java storage library which implements the 
hybrid cloud storage system described in [\[1\]][1].  

Hybris code base is composed by two main modules: MdsManager and KvsManager, 
the first being a thin wrapper layer of the metadata distributed storage service (i.e. Zookeeper),
while the latter implements the storage primitives towards the APIs of the main 
public cloud storage services - currenlty it supports Amazon S3, 
Google Cloud Storage, Rackspace Cloudfiles and Windows Azure Blob.  

In this context, Hybris exposes a simple set of APIs 
and coordinates the two modules in order to provide guarantees of data availability 
and integrity, byzantine fault tolerance and vendor lock-in avoidance.  

For information about technical requirements and development please refer to the [development setup wiki page][2].


 [1]: http://arxiv.org/pdf/1305.4868.pdf                                "BFT Storage with 2t+1 Data Replicas"
 [2]: https://bitbucket.org/pviotti/hybris/wiki/Development%20Setup%20How-To      "Development Setup How-To"

