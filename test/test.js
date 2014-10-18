var path = require('path');
var expect = require('chai').expect;
var level = require('level');
var sublevel = require('level-sublevel');
var rimraf = require('rimraf');
var through2 = require('through2');
var uuid = require('uuid');
var moment = require('moment');
var simpleIndex = require('../index');

var dbPath = path.resolve(__dirname, 'test.db');

rimraf.sync(dbPath);

var db = sublevel(level(dbPath, { valueEncoding: 'json' }));
var thingsTable = simpleIndex(db.sublevel('things'));
var stuffTable = simpleIndex(db.sublevel('stuff'));

function clone (obj) {
	return JSON.parse(JSON.stringify(obj));
}

function createId () {
	return uuid.v4();
}

function padLeft (str, ch, len) {
	return str.length >= len ? str : padLeft(ch + str, ch, len);
}

function timestampKey (time) {
	var t = moment.utc(time);
	return padLeft(t.format('YYYYMMDDHHmmss'), '0', 16) + padLeft(t.format('SSS'), '0', 4);
}

//+ Jonas Raoni Soares Silva
//@ http://jsfromhell.com/array/shuffle [v1.0]
function shuffle(o){ //v1.0
	for (var j, x, i = o.length; i; j = Math.floor(Math.random() * i), x = o[--i], o[i] = o[j], o[j] = x);
	return o;
};

// some data
var things = [
	{ key: createId(), value: { name: 'Thing 0', type: 'toy' } },
	{ key: createId(), value: { name: 'Thing 1', type: 'tool' } },
	{ key: createId(), value: { name: 'Thing 2', type: 'toy' } },
	{ key: createId(), value: { name: 'Thing 3', type: 'toy' } },
	{ key: createId(), value: { name: 'Thing 4', type: 'tool' } },
	{ key: createId(), value: { name: 'Thing 5', type: 'tool' } },
	{ key: createId(), value: { name: 'Thing 6', type: 'toy' } },
	{ key: createId(), value: { name: 'Thing 7', type: 'tool' } },
	{ key: createId(), value: { name: 'Thing 8', type: 'tool' } },
	{ key: createId(), value: { name: 'Thing 9', type: 'toy' } }
];

// some ordered data
var orderedStuff = [
	{ key: timestampKey(moment.utc('2010-01-01')) + '-' + uuid.v4().substr(0, 8), value: { idx: 0, name: 'Stuff 0', type: 'foo' } },
	{ key: timestampKey(moment.utc('2011-02-02')) + '-' + uuid.v4().substr(0, 8), value: { idx: 1, name: 'Stuff 1', type: 'foo' } },
	{ key: timestampKey(moment.utc('2012-03-03')) + '-' + uuid.v4().substr(0, 8), value: { idx: 2, name: 'Stuff 2', type: 'bar' } },
	{ key: timestampKey(moment.utc('2013-04-04')) + '-' + uuid.v4().substr(0, 8), value: { idx: 3, name: 'Stuff 3', type: 'foo' } },
	{ key: timestampKey(moment.utc('2014-05-05')) + '-' + uuid.v4().substr(0, 8), value: { idx: 4, name: 'Stuff 4', type: 'bar' } },
	{ key: timestampKey(moment.utc('2015-06-06')) + '-' + uuid.v4().substr(0, 8), value: { idx: 5, name: 'Stuff 5', type: 'bar' } },
	{ key: timestampKey(moment.utc('2016-07-07')) + '-' + uuid.v4().substr(0, 8), value: { idx: 6, name: 'Stuff 6', type: 'bar' } },
	{ key: timestampKey(moment.utc('2017-08-08')) + '-' + uuid.v4().substr(0, 8), value: { idx: 7, name: 'Stuff 7', type: 'bar' } },
	{ key: timestampKey(moment.utc('2018-09-09')) + '-' + uuid.v4().substr(0, 8), value: { idx: 8, name: 'Stuff 8', type: 'foo' } },
	{ key: timestampKey(moment.utc('2019-10-10')) + '-' + uuid.v4().substr(0, 8), value: { idx: 9, name: 'Stuff 9', type: 'foo' } }
];

// shuffle the ordered data
stuff = shuffle(clone(orderedStuff));

describe('simple-index', function () {
	
	describe('createIndex()', function () {

		var thingsTypeCallbackCalled = false;
		var stuffTypeCallbackCalled = false;

		it('should create an index', function (done) {
			thingsTable.createIndex('type', function (key, value, emit) {
				expect(key).to.exist;
				expect(value).to.be.an.object;
				expect(emit).to.be.a.function;

				thingsTypeCallbackCalled = true;

				if (value.type) { emit(value.type); }
			});

			stuffTable.createIndex('type', function (key, value, emit) {
				expect(key).to.exist;
				expect(value).to.be.an.object;
				expect(emit).to.be.a.function;

				stuffTypeCallbackCalled = true;

				if (value.type) { emit(value.type); }
			});

			done();
		});

		it('should call the index callback when a new value is added', function (done) {
			var thing = things.shift();
			var stuf = stuff.shift();

			thingsTable.put(thing.key, thing.value, function (err) {
				if (err) { console.log(err); }
				expect(thingsTypeCallbackCalled).to.be.true;

				stuffTable.put(stuf.key, stuf.value, function (err) {
					if (err) { console.log(err); }
					expect(stuffTypeCallbackCalled).to.be.true;
					done();
				});
			});

		});

		it('should not create the same index twice', function () {
			thingsTable.createIndex('type', function (key, value, emit) {
			});
		});

	});

	describe('createIndexedStream()', function () {

		before(function (done) {

			function insertThings (callback) {
				var batch = things.map(function (item) {
					return { type: 'put', key: item.key, value: item.value };
				});

				thingsTable.batch(batch, callback);
			}

			function insertStuff (callback) {
				var batch = stuff.map(function (item) {
					return { type: 'put', key: item.key, value: item.value };
				});

				stuffTable.batch(batch, callback);
			}

			insertThings(function () {
				insertStuff(done);
			});
		});

		it('should return a readable stream by an index', function (done) {
			var stream = thingsTable.createIndexStream('type', 'tool');

			var count = 0;

			stream.pipe(through2.obj(function (chunk, encoding, callback) {
				count += 1;
				expect(chunk.key).to.be.ok;
				expect(chunk.value).to.be.an.object;
				expect(chunk.value).to.have.property('type', 'tool');
				callback();
			}));

			stream.on('end', function () {
				expect(count).to.equal(5);
				done();
			});
		});

		it('should return a readable stream by an index with only values', function (done) {
			var stream = thingsTable.createIndexStream('type', 'tool', {
				keys: false,
				values: true
			});

			var count = 0;

			stream.pipe(through2.obj(function (chunk, encoding, callback) {
				count += 1;
				expect(chunk).to.have.property('type', 'tool');
				callback();
			}));

			stream.on('end', function () {
				expect(count).to.equal(5);
				done();
			});
		});

		it('should return a readable stream by an index with only keys', function (done) {
			var stream = thingsTable.createIndexStream('type', 'tool', {
				keys: false,
				values: true
			});

			var count = 0;

			stream.pipe(through2.obj(function (chunk, encoding, callback) {
				count += 1;
				expect(chunk).to.have.property('type', 'tool');
				callback();
			}));

			stream.on('end', function () {
				expect(count).to.equal(5);
				done();
			});
		});

		it('should return a limited readable stream by an index', function (done) {
			var stream = thingsTable.createIndexStream('type', 'tool', {
				limit: 3
			});

			var count = 0;

			stream.pipe(through2.obj(function (chunk, encoding, callback) {
				count += 1;
				callback();
			}));

			stream.on('end', function () {
				expect(count).to.equal(3);
				done();
			});
		});

		it('should not have any results if they value does not exist', function (done) {
			var stream = thingsTable.createIndexStream('type', 'doesn-not-exist');

			var count = 0;

			stream.pipe(through2.obj(function (chunk, encoding, callback) {
				count += 1;
				callback();
			}));

			stream.on('end', function () {
				expect(count).to.equal(0);
				done();
			});
		});

		it('should be sorted by key', function (done) {
			var stream = stuffTable.createIndexStream('type', 'foo', {
			});

			var prevIndex = -1;

			stream.pipe(through2.obj(function (chunk, encoding, callback) {
				expect(chunk.value.idx).to.be.greaterThan(prevIndex);
				prevIndex = chunk.value.idx;
				callback();
			}));

			stream.on('end', function () {
				done();
			});
		});

		it('should return a reversed stream with "reverse": true', function (done) {
			var stream = stuffTable.createIndexStream('type', 'foo', {
				reverse: true
			});

			var prevIndex = stuff.length + 1;

			stream.pipe(through2.obj(function (chunk, encoding, callback) {
				expect(chunk.value.idx).to.be.lessThan(prevIndex);
				prevIndex = chunk.value.idx;
				callback();
			}));

			stream.on('end', function () {
				done();
			});
		});

		it('should respect "gt" option', function (done) {
			var stream = stuffTable.createIndexStream('type', 'bar', {
				gt: timestampKey(moment.utc('2015-01-01'))
			});

			var count = 0;

			stream.pipe(through2.obj(function (chunk, encoding, callback) {
				count += 1;
				callback();
			}));

			stream.on('end', function () {
				expect(count).to.equal(3);
				done();
			});
		});

		it('should respect "gt" and "limit" option', function (done) {
			var stream = stuffTable.createIndexStream('type', 'foo', {
				gt: timestampKey(moment.utc('2014-01-01')),
				limit: 1
			});

			var count = 0;

			stream.pipe(through2.obj(function (chunk, encoding, callback) {
				count += 1;
				callback();
			}));

			stream.on('end', function () {
				expect(count).to.equal(1);
				done();
			});
		});

		it('should respect "lt" option', function (done) {
			var stream = stuffTable.createIndexStream('type', 'bar', {
				lt: timestampKey(moment.utc('2015-01-01')) + '\xff'
			});

			var count = 0;

			stream.pipe(through2.obj(function (chunk, encoding, callback) {
				count += 1;
				callback();
			}));

			stream.on('end', function () {
				expect(count).to.equal(2);
				done();
			});
		});

		it('should respect "lt" and "limit" option', function (done) {
			var stream = stuffTable.createIndexStream('type', 'bar', {
				lt: timestampKey(moment.utc('2016-01-01')),
				limit: 2
			});

			var count = 0;

			stream.pipe(through2.obj(function (chunk, encoding, callback) {
				count += 1;
				callback();
			}));

			stream.on('end', function () {
				expect(count).to.equal(2);
				done();
			});
		});

		it('should respect "gt" and "reverse" option', function (done) {
			var stream = stuffTable.createIndexStream('type', 'foo', {
				gt: timestampKey(moment.utc('2014-01-01')),
				reverse: true
			});

			var count = 0;
			var prevIndex = stuff.length + 1;

			stream.pipe(through2.obj(function (chunk, encoding, callback) {
				expect(chunk.value.idx).to.be.lessThan(prevIndex);
				prevIndex = chunk.value.idx;
				count += 1;
				callback();
			}));

			stream.on('end', function () {
				expect(count).to.equal(2);
				done();
			});
		});

		it('should respect "lt" and "reverse" option', function (done) {
			var stream = stuffTable.createIndexStream('type', 'foo', {
				lt: timestampKey(moment.utc('2014-01-01')),
				reverse: true
			});

			var count = 0;
			var prevIndex = stuff.length + 1;

			stream.pipe(through2.obj(function (chunk, encoding, callback) {
				expect(chunk.value.idx).to.be.lessThan(prevIndex);
				prevIndex = chunk.value.idx;
				count += 1;
				callback();
			}));

			stream.on('end', function () {
				expect(count).to.equal(3);
				done();
			});
		});
		//

		it('should respect "gte" option', function (done) {
			var stream = stuffTable.createIndexStream('type', 'bar', {
				gte: orderedStuff[5].key
			});

			var count = 0;

			stream.pipe(through2.obj(function (chunk, encoding, callback) {
				count += 1;
				callback();
			}));

			stream.on('end', function () {
				expect(count).to.equal(3);
				done();
			});
		});

		it('should respect "gte" and "limit" option', function (done) {
			var stream = stuffTable.createIndexStream('type', 'foo', {
				gte: orderedStuff[5].key,
				limit: 2
			});

			var count = 0;

			stream.pipe(through2.obj(function (chunk, encoding, callback) {
				count += 1;
				callback();
			}));

			stream.on('end', function () {
				expect(count).to.equal(2);
				done();
			});
		});

		it('should respect "lte" option', function (done) {
			var stream = stuffTable.createIndexStream('type', 'bar', {
				lte: orderedStuff[4].key
			});

			var count = 0;

			stream.pipe(through2.obj(function (chunk, encoding, callback) {
				count += 1;
				callback();
			}));

			stream.on('end', function () {
				expect(count).to.equal(2);
				done();
			});
		});

		it('should respect "lte" and "limit" option', function (done) {
			var stream = stuffTable.createIndexStream('type', 'bar', {
				lte: orderedStuff[7].key,
				limit: 2
			});

			var count = 0;

			stream.pipe(through2.obj(function (chunk, encoding, callback) {
				count += 1;
				callback();
			}));

			stream.on('end', function () {
				expect(count).to.equal(2);
				done();
			});
		});

		it('should respect "gte" and "reverse" option', function (done) {
			var stream = stuffTable.createIndexStream('type', 'foo', {
				gte: orderedStuff[5].key,
				reverse: true
			});

			var count = 0;
			var prevIndex = stuff.length + 1;

			stream.pipe(through2.obj(function (chunk, encoding, callback) {
				expect(chunk.value.idx).to.be.lessThan(prevIndex);
				prevIndex = chunk.value.idx;
				count += 1;
				callback();
			}));

			stream.on('end', function () {
				expect(count).to.equal(2);
				done();
			});
		});

		it('should respect "lte" and "reverse" option', function (done) {
			var stream = stuffTable.createIndexStream('type', 'foo', {
				lte: orderedStuff[7].key,
				reverse: true
			});

			var count = 0;
			var prevIndex = stuff.length + 1;

			stream.pipe(through2.obj(function (chunk, encoding, callback) {
				expect(chunk.value.idx).to.be.lessThan(prevIndex);
				prevIndex = chunk.value.idx;
				count += 1;
				callback();
			}));

			stream.on('end', function () {
				expect(count).to.equal(3);
				done();
			});
		});

	});

	describe('updateIndex()', function () {
	
		it('should rebuild a new index', function (done) {
			var tmpTable = simpleIndex(db.sublevel('tmp'));

			function insertThings (callback) {
				var batch = things.map(function (item) {
					return { type: 'put', key: item.key, value: item.value };
				});

				tmpTable.batch(batch, callback);
			}

			insertThings(function () {
				tmpTable.createIndex('type', function (key, value, emit) {
					if (value.type) { emit(value.type); }
				});

				tmpTable.updateIndex('type', function () {
					var count = 0;
					var stream = tmpTable.createIndexStream('type', 'tool');

					stream.pipe(through2.obj(function (chunk, encoding, callback) {
						count += 1;
						callback();
					}));

					stream.on('end', function () {
						expect(count).to.equal(5);
						done();
					});
				});
			});
		});

		it('should rebuild an existing index', function (done) {
			thingsTable.updateIndex('type', function () {
				var count = 0;
				var stream = thingsTable.createIndexStream('type', 'toy');

				stream.pipe(through2.obj(function (chunk, encoding, callback) {
					count += 1;
					callback();
				}));

				stream.on('end', function () {
					expect(count).to.equal(5);
					done();
				});
			});
		});
	
	});

	describe('deleting', function () {
	
		it('should unindex when something gets deleted', function (done) {
			thingsTable.del(things[3].key, function (err) {
				var stream = thingsTable.createIndexStream('type', things[3].value.type);
				var count = 0;

				stream.pipe(through2.obj(function (chunk, encoding, callback) {
					count += 1;
					callback();
				}));

				stream.on('end', function () {
					expect(count).to.equal(4);
					done();
				});
			});
		})
	
	});

	describe('dropIndex()', function () {

		it('should drop an index', function () {
			thingsTable.dropIndex('type', function () {
				var count = 0;
				var stream = thingsTable.createIndexStream('type', 'toy');

				stream.pipe(through2.obj(function (chunk, encoding, callback) {
					count += 1;
					callback();
				}));

				stream.on('end', function () {
					expect(count).to.equal(0);
					done();
				});
			});
		});

	});

});
