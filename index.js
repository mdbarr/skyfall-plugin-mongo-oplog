'use strict';

const async = require('async');
const MongoClient = require('mongodb').MongoClient;

const events = {
  i: 'insert',
  u: 'update',
  d: 'delete'
};

function MongoOplog (skyfall, options) {
  this.id = skyfall.utils.id();

  this.configure = (config) => {
    this.url = config.url || this.url || 'mongodb://127.0.0.1:27017/local';
    this.retry = config.retry || this.retry || 5000;
    this.slowStart = config.slowStart || this.slowStart || 0;
    this.collection = config.collection || this.collection || 'oplog.rs';
  };

  this.connect = () => {
    skyfall.event.emit({
      type: 'mongo:oplog:connecting',
      data: { url: this.url },
      source: this.id
    });

    return setTimeout(() => {
      return async.until((test) => {
        this.client = new MongoClient(this.url, {
          useNewUrlParser: true,
          useUnifiedTopology: true
        });

        return this.client.connect((error) => {
          if (error) {
            return test(null, false);
          }
          return test(null, true);
        });
      }, (next) => {
        return setTimeout(() => {
          return next(null);
        }, this.retry || 5000);
      }, (error) => {
        if (error) {
          return skyfall.event.emit({
            type: 'mongo:oplog:error',
            data: error,
            source: this.id
          });
        }

        this.db = this.client.db();

        this.oplog = this.db.collection(this.collection);

        this.tail();

        return skyfall.event.emit({
          type: 'mongo:oplog:tailing',
          data: { url: this.url },
          source: this.id
        });
      });
    }, this.slowStart);
  };

  this.tail = () => {
    this.stream = this.oplog.find({ }, {
      awaitdata: true,
      noCursorTimeout: true,
      numberOfRetries: Number.MAX_VALUE,
      tailable: true
    }).
      stream();

    this.stream.on('data', (document) => {
      skyfall.events.emit({
        type: 'mongo:oplog:op',
        data: document,
        id: this.id
      });

      const type = events[document.op];
      if (type) {
        skyfall.events.emit({
          type: `mongo:oplog:${ type }`,
          data: document,
          source: this.id
        });

        if (document.ns) {
          skyfall.events.emit({
            type: `oplog:${ document.ns }:${ type }`,
            data: document.o,
            source: this.id
          });
        }
      }
    });

    this.stream.on('end', () => {
      setTimeout(this.tail, this.retry);
    });
  };

  if (Object.keys(options).length) {
    this.configure(options);
  }
}

module.exports = {
  name: 'mongo-oplog',
  install: (skyfall, options) => {
    skyfall.mongoOplog = new MongoOplog(skyfall, options);
  }
};
