### Hybris

Hybris is a key-value hybrid cloud storage system that robustly replicates data 
over untrusted public clouds while keeping metadata on trusted private premises.  
Thanks to this design, Hybris provides **strong consistency** guarantees (i.e., linearizability) 
and affordable **Byzantine fault tolerance** 
(i.e., withstanding f faulty clouds with as few as **2f+1** replicas).  
Hybris is also very **efficient**: in the common case it writes to f+1 clouds and it reads from one single cloud.

![Hybris architecture](https://raw.github.com/pviotti/hybris/master/doc/hybris-architecture.png)

For more information and detailed benchmarks read [our SoCC14 paper][1].


### Code base overview

Hybris code base is composed by two main modules: MdsManager and KvsManager, 
the first being a thin wrapper layer of the metadata distributed storage service (i.e. ZooKeeper),
while the latter implements the storage primitives towards the APIs of the main 
public cloud storage services - currently, it supports Amazon S3, 
Google Cloud Storage, Rackspace Cloud Files and Windows Azure Blob.  

Maven is used for building and managing dependencies.  

Read the [wiki][2] for more information on development setup.


### Authors and license

Hybris has been developed at [EURECOM][3] as part of the [CloudSpaces][4] European research project.  
Its code is released under the terms of Apache 2.0 license.  

Erasure coding support is provided by the [jerasure][5] library and its [JNI bindings][6].


 [1]: http://www.eurecom.fr/en/publication/4414/detail/hybris-robust-hybrid-cloud-storage
 [2]: https://github.com/pviotti/hybris/wiki/Development-Setup-How-To
 [3]: http://www.eurecom.fr
 [4]: http://cloudspaces.eu/
 [5]: http://web.eecs.utk.edu/~plank/plank/papers/CS-08-627.html
 [6]: https://github.com/jvandertil/Jerasure