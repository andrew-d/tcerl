# Durability #

Dets makes some effort to ensure that, should the beam crash, writes made to a dets end up in the file and the file is not corrupted.  It is not perfect but it is good enough that probably you've never noticed a problem.

Tokyocabinet goes for speed and so most operations are done to the in-memory structure and have to be explicitly synchronized to be durable.  Since mnesia's transaction manager sits above the storage layer and it assumes that when it told the storage layer to do something that it gets done, it cannot assist in this regard.

Tokyocabinet now supports the use of transactions along with the BDBOTSYNC flag, which would provide both the requisite durability and consistency.  I have not done the work to optionally enable this mode of operation, due to laziness fundamentally: when I started the project Tokyocabinet didn't have these options, and because I use mnesia in distributed mode I get fresh table copies of everything from another node when I reboot so it hasn't been an itch I need to scratch.

As a really crappy workaround you can manually sync a database foo via
```
tcbdbets:sync (element (2, tcbdbsrv:get_tab (foo)))
```

If you have a consulting budget and need a better fix, don't hesitate to [contact me](mailto:paul@mineiro.com).