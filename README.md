# Hybris [![Build Status](https://travis-ci.org/pviotti/hybris.svg?branch=master)](https://travis-ci.org/pviotti/hybris)

Hybris is a key-value hybrid cloud storage system that robustly replicates data 
over untrusted public clouds while keeping metadata on trusted private premises.  
Thanks to this design, Hybris provides **strong consistency** guarantees (i.e., linearizability) 
and affordable **Byzantine fault tolerance** 
(i.e., withstanding f faulty clouds with as few as **2f+1** replicas).  
Hybris is also very **efficient**: in the common case it writes to f+1 clouds and it reads from one single cloud.

![Hybris architecture](http://i.imgur.com/YyAYB5y.png)

For more information and detailed benchmarks read [our SoCC14 paper][1].   
A version of the personal cloud application [StackSync][7] featuring Hybris as storage backend 
is available [here][8].

## Code base overview

The code base of Hybris is composed by two main modules: MdsManager and KvsManager; 
the first is a thin wrapper layer of the metadata distributed storage service 
(i.e. ZooKeeper or Consul), while KvsManager implements the storage primitives 
towards the APIs of the main public cloud storage services - 
currently, it supports Amazon S3, Google Cloud Storage, Rackspace Cloud Files 
and Windows Azure Blob.  

Maven is used to compile the code and manage the dependencies.   

Read the [wiki][2] for more information on development setup.

## Authors and license

Hybris has been developed at [EURECOM][3] as part of the [CloudSpaces][4] European research project.
Its code is released under the terms of Apache 2.0 license.  

Erasure coding support is provided by the [Jerasure][5] library through its [JNI bindings][6].


 [1]: http://www.eurecom.fr/en/publication/4414/detail/hybris-robust-hybrid-cloud-storage
 [2]: https://github.com/pviotti/hybris/wiki/Development-Setup-How-To
 [3]: http://www.eurecom.fr
 [4]: http://cloudspaces.eu
 [5]: http://web.eecs.utk.edu/~plank/plank/papers/CS-08-627.html
 [6]: https://github.com/jvandertil/Jerasure
 [7]: http://stacksync.org
 [8]: https://github.com/pviotti/stacksync-desktop
