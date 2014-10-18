level-simple-index
==================

[![Build Status](https://secure.travis-ci.org/maxkueng/level-simple-index.png?branch=master)](http://travis-ci.org/maxkueng/level-simple-index)

A simple mapped index for leveldb / levelup that doesn't mess with your streams.

## Usage

### Install

```
npm install level-simple-index --save
```

### Creating an index


```javascript
var level = require('level');
var simpleIndex = require('level-simple-index');
var db = simpleIndex(level('data.db', { valueEncoding: 'json' }));

// "type" is the name of the index
// `mapFunction` is called every time before data is inserted or removed
//    - It receives the key and the value of the record
//    - Call `emit` to store a value under the given index
db.createIndex('type', function mapFunction (key, value, emit) {
  if (value.type) { emit(value.type); }
});
```

### Getting indexed data

```javascript
// "type" is the name of the index
// "toy" is the value to look up
// the optional `options` argument supports all of levelup's
//   options: gt, gte, lt, lte, reverse, limit, keys, values
var stream = db.createIndexedStream('type', 'toy', {
  gt: '\x00',
  lt: '\xff',
  limit: 5,
  reverse: true
});

stream.on('data', function onData (data) {
  console.log(data.key);
  console.log(data.value);
});

stream.on('end', noop);
```

### Rebuilding an index

Drops all the indexed data and rebuilds it.

```javascript
db.updateIndex('type', function rebuilt (err) {
  console.log('index rebuild');
});
```

### Dropping an index

Note: Currently doesn't unregister the map functions so it will build up again over time.

```javascript
db.dropIndex('type', function dropped (err) {
  console.log('index dropped');
});
```

## License

MIT License

Copyright (c) 2014 Max Kueng

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
