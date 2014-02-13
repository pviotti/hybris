### Hybris

Hybris is a key-value hybrid cloud storage system that robustly replicates data over untrusted public clouds while keeping metadata on trusted private premises.  
This design allows for strong consistency guarantees (i.e., linearizability) and affordable Byzantine fault tolerance (i.e., withstanding f faulty clouds with as few as 2f+1 replicas).  
Hybris is also very efficient: in the common case it writes to f+1 clouds and read from only a single cloud.

For more information and detailed benchmarks, check [our research paper](http://www.eurecom.fr/en/publication/4157).


### Code base overview

Hybris code base is composed by two main modules: MdsManager and KvsManager, 
the first being a thin wrapper layer of the metadata distributed storage service (i.e. Zookeeper),
while the latter implements the storage primitives towards the APIs of the main 
public cloud storage services - currently, it supports Amazon S3, 
Google Cloud Storage, Rackspace Cloudfiles and Windows Azure Blob.  

Maven is used for building and managing dependencies.  

Read the [wiki](https://github.com/pviotti/hybris/wiki/Development-Setup-How-To) for more information on development setup.


### Authors and license

Hybris has been developed at [EURECOM](http://www.eurecom.fr) as part of the [CloudSpaces](http://cloudspaces.eu/) European research project.  
Its code is released under the terms of Apache 2.0 license.

