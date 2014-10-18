"use strict";

var sublevel = require('level-sublevel');
var bytewise = require('bytewise');
var hooks = require('level-hooks');
var through2 = require('through2');
var assign = require('object-assign');

var SEPARATOR = '!';

exports = module.exports = simpleIndex;

function simpleIndex (db) {
	var hasSublevel = (typeof db.sublevel === 'function');

	if (!hasSublevel) {
		db = sublevel(db);
	}

	hookItUp(db, createIndex);
	hookItUp(db, updateIndex);
	hookItUp(db, dropIndex);
	hookItUp(db, createIndexStream);

	if (!db.indexes && !db.indexesDB) {
		hooks(db);
		db.indexes = {};
		db.indexesDB = db.sublevel('_indexes', { valueEncoding: 'utf8' });
	}

	return db;
}

function noop () {}

function hookItUp (db, fn) {
	if (!db[fn.name]) {
		db[fn.name] = fn.bind(null, db);
	}
}

function encodeKey (key) {
	return bytewise.encode(key).toString('hex');
}

function decodeKey (key) {
	return bytewise.decode(new Buffer(key, 'hex'));
}

function constructKey (indexName, indexValue, key) {
	key = key || '';
	return indexName + SEPARATOR + indexValue + SEPARATOR + key;
}

function createIndex (db, indexName, mapFunction) {
	mapFunction = mapFunction || noop;

	if (db.indexes[indexName]) {
		return;
	}

	db.indexes[indexName] = {
		name: indexName,
		mapFunction: mapFunction
	};

	db.hooks.pre(
		{ start: '\x00', end: '\xff' },
		function (change, add) {
			if (change.type === 'put') {
				indexChange(change);

			} else if (change.type === 'del') {
				unindexChange(change);
			}
		}
	);

	function indexChange (change) {
		function emit (indexValue) {
			var indexKey = encodeKey(constructKey(indexName, indexValue, change.key));
			db.indexesDB.put(indexKey, change.key, function (err) {
				if (err) {  }
			})
		}

		mapFunction.call(db, change.key, change.value, emit);
	}

	function unindexChange (change) {
		function emit (indexValue) {
			var indexKey = encodeKey(constructKey(indexName, indexValue, change.key));
			db.indexesDB.del(indexKey, function (err) {
				if (err) {  }
			})
		}

		db.get(change.key, function (err, value) {
			if (err) { return; }

			mapFunction.call(db, change.key, value, emit);
		});
	}
}

function dropIndex (db, indexName, callback) {
	// unregister hooks here?
	clearIndex(db, indexName, callback);
}

function clearIndex (db, indexName, callback) {
	var stream = db.indexesDB.createReadStream({
		gt: encodeKey(indexName),
		lt: encodeKey(indexName + '\xff'),
		keys: true,
		values: false
	});

	stream.pipe(through2.obj(function (key, encoding, next) {
		db.indexesDB.del(key, next);
	}));

	stream.on('end', callback);
}

function updateIndex (db, indexName, callback) {
	var index = db.indexes[indexName];

	if (!index) { return callback(new Error('Index "' + indexName + '" does not exist')); }

	var mapFunction = index.mapFunction;

	function indexChange (change, callback) {
		function emit (indexValue) {
			var indexKey = encodeKey(constructKey(indexName, indexValue, change.key));
			db.indexesDB.put(indexKey, change.key, function (err) {
				if (err) {  }
				callback();
			})
		}

		mapFunction.call(db, change.key, change.value, emit);
	}

	clearIndex(db, indexName, function () {
		var stream = db.createReadStream({
			keys: true,
			values: true
		});

		stream.pipe(through2.obj(function (chunk, encoding, next) {
			indexChange(chunk, next);
		}));

		stream.on('end', function () {
			callback();
		});
	});

}

function createIndexStream (db, indexName, indexValue, options) {
	var defaults = {
		reverse: false,
		limit: undefined,
		gt: undefined,
		lt: undefined,
		keys: true,
		values: true
	};

	options = assign(false, defaults, options);

	var indexKey = constructKey(indexName, indexValue);

	var gt, gte, lt, lte, start, end;

	if (options.gt) { gt = encodeKey(indexKey + options.gt); }
	if (options.gte) { gte = encodeKey(indexKey + options.gte); }
	if (options.lt) { lt = encodeKey(indexKey + options.lt); }
	if (options.lte) { lte = encodeKey(indexKey + options.lte); }

	// DEPRECATED
	if (options.start) { start = encodeKey(indexKey + options.start); }
	if (options.end) { end = encodeKey(indexKey + options.end); }

	if (!gt && !gte && !start) { gt = encodeKey(indexKey); }
	if (!lt && !lte && !end) { lt = encodeKey(indexKey + '\xff'); }

	var streamOptions = {
		gt: gt,
		gte: gte,
		lt: lt,
		lte: lte,
		keys: false,
		values: true,
		reverse: options.reverse,
		limit: options.limit ? options.limit : null
	};

	return db.indexesDB.createReadStream(streamOptions)
		.pipe(through2.obj(function (key, encoding, callback) {
			var stream = this;

			db.get(key, function (err, value) {
				if (!err) {
					var chunk = { key: key, value: value };
					if (options.keys === true && options.values === false) {
						chunk = key;
					} else if (options.keys === false && options.values === true) {
						chunk = value;
					}

					stream.push(chunk);
				}
				callback();
			});
		}));
}
