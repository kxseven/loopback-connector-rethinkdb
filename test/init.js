module.exports = require('should');

var DataSource = require('loopback-datasource-juggler').DataSource;

var config = {
	host: "localhost",
	port: 28015,
	db: "test"
};

process.env.TESTING = process.env.CI || process.env.NODE_ENV;

global.getDataSource = global.getSchema = function (customConfig) {
	var db = new DataSource(require('../'), customConfig || config);
	db.log = function (a) {
		console.log(a);
	};

	return db;
};
