# Memory Usage #

Tokyocabinet keeps a cache of leaves that can grow quite big by default.  By playing with the `leaf_node_cache` and `nonleaf_node_cache` parameters you can control this growth in exchange for more I/O.  Note these are the size of the caches in units of leaves; the number of entries in a leaf is controlled by the related parameters `leaf_members` and `non_leaf_members`.

Check the documentation for the open methods to see how to specify these parameters.