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
 * jClouds 1.6

Development tools:

 * Maven 3
 * Git 1.8
 * Eclipse, with [Egit][2] and [m2e][3] plugins
     * for installing them on Eclipse EE: menu "Help" -> "Eclipse Marketplace"
     * Git guides: [starting guide][4], [Git vs SVN][5], [cheatsheet][6], [official docs][7]
     * Maven guides: [starting guide][8], [Introduction][9], [tutorials][10], [repository search engine][11]
     * Egit plugin guides: [guide 1][12], [guide 2][13]
 

 [1]: http://arxiv.org/abs/1305.4868            "BFT Storage with 2t+1 Data Replicas - C. Cachin, D. Dobre, M. Vukolic"
 [2]: http://www.eclipse.org/egit/              "Egit Eclipse plugin"
 [3]: http://eclipse.org/m2e/                   "m2e Eclipse plugin"
 [4]: http://rogerdudler.github.com/git-guide/  "Git starting guide"
 [5]: http://git.or.cz/course/svn.html          "Git vs SVN crash course"
 [6]: http://ndpsoftware.com/git-cheatsheet.html    "Git cheatsheet"
 [7]: http://git-scm.com/documentation          "Git Official docs"
 [8]: http://maven.apache.org/guides/getting-started/maven-in-five-minutes.html "Maven starting guide"
 [9]: http://maven.apache.org/guides/getting-started/index.html "Maven Introduction"
 [10]: http://www.mkyong.com/tutorials/maven-tutorials/ "Maven Tutorials"
 [11]: http://mvnrepository.com/                "Maven repository search engine"
 [12]: http://wiki.eclipse.org/EGit/User_Guide  "Egit guide 1"
 [13]: http://www.vogella.com/articles/EGit/article.html  "Egit guide 2"
