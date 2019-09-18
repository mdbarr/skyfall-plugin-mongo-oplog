'use strict';

function MongoOplog (skyfall) {
  this.id = skyfall.utils.id();
}

module.exports = {
  name: 'mongo-oplog',
  install: (skyfall, options) => {
    skyfall.mongoOplog = new MongoOplog(skyfall, options);
  }
};
