# mccouch

No, not [this mccouch](https://github.com/mccouch), but a memcached
binary protocol interface to [CouchDB][couchdb].

This is used by Couchbase Server to provide an efficient means of
moving data between from [ep-engine][epe] and [CouchDB][couchdb].

If you want to build and hack on this, it is recommended to start with
the [couchdb manifest build][manifest].  That'll get it all together
for you.


[couchdb]: http://couchdb.apache.org/
[couchbase]: http://www.couchbase.com/
[epe]: https://github.com/membase/ep-engine
[manifest]: https://github.com/couchbase/couchdb-manifest
