dbInterrogator
==============

A collection of OSGi services that provide interrogation methods for common database
platforms.

What started life as a java alternative to MySQLDump soon evolved into a support
library for dbRecorder, a database versioning tool.

Over time I have been slowly adding support for other database platforms and so I
decided it was time to wrap up the individual libraries into something more
coherent, hence the umbrella project - dbInterrogator.

Requirements
------------

* Java 1.6 (or above)
* Maven 3

Contributing
------------

Please do! Go on, don't be shy.

1. Create an [Issue] that clearly describes:
     * the problem you are trying to solve
     * an outline of your proposed solution
2. Wait a little while for any feedback
3. [Fork] dbInterrogator into your very own GitHub repository
4. Create a topic branch with a name corresponding to the issue number
   from step 1 e.g #XXX:
     * `$ git clone git@github.com/wave2/dbinterrogator.git my-dbinterrogator-repo`
     * `$ cd my-dbinterrogator-repo`
     * `$ git checkout -b dbinterrogator-XXX`
5. Commit your changes and include the issue number in your
   commit message:
     * `$ git commit -am "[#XXX] Added something cool"`
6. Push your changes to the branch:
     * `$ git push origin dbinterrogator-XXX`
7. Send a [Pull Request] including the issue number in the subject

License
-------

Copyright &copy; 2007-2011 Wave2 Limited. All rights reserved. Licensed under [BSD License].

[BSD License]: https://github.com/wave2/dbinterrogator/raw/master/LICENSE
[Fork]: http://help.github.com/fork-a-repo
[Issue]: https://github.com/wave2/dbinterrogator/issues
[Pull Request]: http://help.github.com/pull-requests