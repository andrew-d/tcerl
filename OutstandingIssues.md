# Introduction #

This has been a one-man project, pretty much, which unfortunately means the itches that get scratched are the ones that I run into :).

Consequently, there are lots of open items.  Here are the ones I know about:

  * [Durability](http://code.google.com/p/tcerl/wiki/Durability): I use Mnesia in distributed mode on EC2, so when I lose a node I get a fresh copy of a table from another node.  Hence, I've never really bolted down the durability story.
  * Erlang Release 13 support: I use Release 12 so there hasn't been a need.  I'm pretty conservative about using the latest versions of the Erlang emulator for production systems because I like to see the bugs shaken out. You can try and adjust the patches to work with Release 13.  If you get something that works you can submit and I'll incorporate it. Or you can wait until I start using Release 13 myself.
  * Set support: With the latest versions of Tokyocabinet it looks like support for a set-type table is possible.

If you have a consulting budget, and you want these items addressed promptly, don't hesitate to [contact me](mailto:paul@mineiro.com).