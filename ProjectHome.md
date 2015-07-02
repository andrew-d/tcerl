[tokyocabinet](http://tokyocabinet.sourceforge.net/) is a DBM style library which boasts excellent space and time efficiency.

This project provides a low-level port of tokyocabinet to Erlang via a linked-in driver, which supports storage of binaries. It also provides a high level erlang term store interface which mimics dets very closely.  The erlang term store can serve as an mnesia table via [mnesiaex](http://code.google.com/p/mnesiaex).

Check out the [ChangeLog](http://code.google.com/p/tcerl/source/browse/trunk/tcerl/ChangeLog).

Another [Dukes of Erl](http://dukesoferl.blogspot.com) release.