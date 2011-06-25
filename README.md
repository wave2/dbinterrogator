MySQLDump Java Library
======================

Originally developed as a Java replacement for the mysqldump command line application.

The library currently provides functionality for exporting DDL with DML exports in need
of some development.

Requirements
------------

* Java 1.6 (or above)
* Maven

Testing
-------

Edit the test properties file (src/test/resources/test.properties)
to point to your test MySQL instance.

* test.hostname
* test.username
* test.password
* test.schema

The tests automatically create and drop the schema specified so make sure you have
full privileges on that schema.

To run the tests:

    $ mvn test

Contributing
------------

Please do! Go on, don't be shy.

1. Create an [Issue] that clearly describes:
     * the problem you are trying to solve
     * an outline of your proposed solution
2. Wait a little while for any feedback
3. [Fork] MySQLDump into your very own GitHub repository
4. Create a topic branch with a name corresponding to the issue number
   from step 1 e.g #XXX:
     * `$ git clone git@github.com/wave2/mysqldump.git my-mysqldump-repo`
     * `$ cd my-mysqldump-repo`
     * `$ git checkout -b mysqldump-XXX`
5. Commit your changes and include the issue number in your
   commit message:
     * `$ git commit -am "[#XXX] Added something cool"`
6. Push your changes to the branch:
     * `$ git push origin mysqldump-XXX`
7. Send a [Pull Request] including the issue number in the subject

License
-------

Copyright &copy; 2007-2011 Wave2 Limited. All rights reserved. Licensed under [BSD License].

[BSD License]: https://github.com/wave2/mysqldump/raw/master/LICENSE
[Fork]: http://help.github.com/fork-a-repo
[Issue]: https://github.com/wave2/mysqldump/issues
[Pull Request]: http://help.github.com/pull-requests