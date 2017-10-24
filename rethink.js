const rethinkdbdash = require('rethinkdbdash');
const moment = require('moment');
const async = require('async');
const _ = require('lodash');
const util = require('util');
const Promise = require('bluebird');
const Rx = require('rx');

const Connector = require('loopback-connector').Connector;

let r = null;

exports.initialize = function initializeDataSource(dataSource, callback) {
	const s = dataSource.settings;

	const rethinkdbdashSettings = Object.assign(
		{
			pool: false,
			pingInterval: 120
		},
		s.rethinkdbdash
	);

	r = rethinkdbdash(rethinkdbdashSettings);

	if (dataSource.settings.rs) {
		s.rs = dataSource.settings.rs;
		if (dataSource.settings.url) {
			const uris = dataSource.settings.url.split(',');
			s.hosts = [];
			s.ports = [];
			uris.forEach(uri => {
				const url = require('url').parse(uri);

				s.hosts.push(url.hostname || 'localhost');
				s.ports.push(parseInt(url.port || '28015', 10));

				if (!s.database) s.database = url.pathname.replace(/^\//, '');
				if (!s.username) s.username = url.auth && url.auth.split(':')[0];
				if (!s.password) s.password = url.auth && url.auth.split(':')[1];
			});
		}

		s.database = s.database || 'test';
	} else {
		if (dataSource.settings.url) {
			const url = require('url').parse(dataSource.settings.url);
			s.host = url.hostname;
			s.port = url.port;
			s.database = (url.pathname || '').replace(/^\//, '');
			s.username = url.auth && url.auth.split(':')[0];
			s.password = url.auth && url.auth.split(':')[1];
		}

		s.host = s.host || 'localhost';
		s.port = parseInt(s.port || '28015', 10);
		s.database = s.database || 'test';
	}

	s.safe = s.safe || false;

	dataSource.adapter = new RethinkDB(s, dataSource, r);
	dataSource.connector = dataSource.adapter;

	if (callback) {
		dataSource.connector.connect(callback);
	}

	process.nextTick(callback);
};

class RethinkDB extends Connector {
	constructor(s, dataSource, r) {
		super();
		Connector.call(this, 'rethink', s);
		this.dataSource = dataSource;
		this.database = s.database;
		this.r = r;
		this.automigrate = _.debounce(this.automigrate, 4000);
	}

	connect() {
		return this.getConnectionInstance();
	}

	getConnectionInstance(force = false) {
		const _this = this;
		const s = _this.settings;
		const dbClient = this.db;

		const isPending = this.connectPromise &&
			this.connectPromise.isPending &&
			this.connectPromise.isPending();

		if (isPending) {
			if (!force) return this.connectPromise;
			// this.connectPromise.cancel()
		}

		const isDisconnected = !dbClient || !dbClient.open;

		const createConnection = () => {
			this.connectPromise = new Promise((resolve, reject) => {
				const cOpts = Object.assign(
					{
						host: s.host,
						port: s.port,
						authKey: s.password
					},
					s.additionalSettings
				);
				if (cOpts.ssl && cOpts.ssl.ca) {
					// check if is a base64 encoded string
					if (
						/^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)$/.test(
							cOpts.ssl.ca
						)
					) {
						cOpts.ssl.ca = Buffer.from(cOpts.ssl.ca, 'base64');
					}
				}

				return r
					.connect(cOpts)
					.then(client => {
						_this.db = client;
						return resolve(client);
					})
					.catch(reject);
			});

			return this.connectPromise;
		};

		if (isDisconnected) return createConnection();

		return this.connectPromise;
	}

	run(query, options) {
		return this.getConnectionInstance().then(client => query.run(client, options));
	}

	getTypes() {
		return ['db', 'nosql', 'rethinkdb'];
	}

	getDefaultIdType() {
		return String;
	}

	tableName(model) {
		const modelClass = this._models[model];
		if (modelClass.settings.rethinkdb) {
			model = _.get(modelClass, 'settings.rethinkdb.collection') ||
				modelClass.settings.plural ||
				model;
		}
		return model;
	}

	// Override define model function
	define(modelDefinition) {
		modelDefinition.settings = modelDefinition.settings || {};
		this._models[modelDefinition.model.modelName] = modelDefinition;
		// this.autoupdate(modelDefinition.model.modelName,function(){})
	}

	// drops tables and re-creates them
	automigrate(models, done) {
		const _this = this;

		const automigrate = client => {
			if (!done && typeof models === 'function') {
				done = models;
				models = undefined;
			}

			// First argument is a model name
			if (typeof models === 'string') {
				models = [models];
			}

			models = _.uniq(models || Object.keys(_this._models));

			r.db(_this.database).tableList().run(client, (error, list) => {
				if (error) {
					return done(error);
				}
				const migratedTables = [];

				async.eachSeries(
					models,
					(model, cb) => {
						const tableName = _this.tableName(model);
						if (!migratedTables.includes(tableName)) {
							migratedTables.push(tableName);
							r.db(_this.database).tableDrop(tableName).run(client, error => {
								r
									.db(_this.database)
									.tableCreate(tableName)
									.run(client, error => {
										if (error) return cb(error);
										cb();
									});
							});
						} else {
							cb();
						}
					},
					err => {
						if (err) return done(e);
						_this.autoupdate(models, done);
					}
				);
			});
		};

		this.getConnectionInstance().then(automigrate);
	}

	autoupdate(models, cb) {
		const _this = this;
		this.getConnectionInstance().then(() => {
			if (_this.debug) {
				debug('autoupdate');
			}
			if (!cb && typeof models === 'function') {
				cb = models;
				models = undefined;
			}
			// First argument is a model name
			if (typeof models === 'string') {
				models = [models];
			}

			models = models || Object.keys(_this._models);

			const enableGeoIndexing = this.settings.enableGeoIndexing === true;

			async.each(
				models,
				(model, modelCallback) => {
					const indexes = _this._models[model].settings.indexes || [];
					let indexList = [];
					let index = {};
					let options = {};

					if (typeof indexes === 'object') {
						for (const indexName in indexes) {
							index = indexes[indexName];
							if (index.keys) {
								// The index object has keys
								options = index.options || {};
								index.options = options;
							} else {
								index = {
									keys: index,
									options: {}
								};
							}
							index.name = indexName;
							indexList.push(index);
						}
					} else if (Array.isArray(indexes)) {
						indexList = indexList.concat(indexes);
					}

					if (_this.debug) {
						debug('create indexes: ', indexList);
					}

					r.db(_this.database)
						.table(_this.tableName(model))
						.indexList()
						.run(_this.db, (error, alreadyPresentIndexes) => {
							if (error) return cb(error);

							async.each(
								indexList,
								(index, indexCallback) => {
									if (_this.debug) {
										debug('createIndex: ', index);
									}

									if (alreadyPresentIndexes.includes(index.name)) {
										return indexCallback();
									}

									let query = r
										.db(_this.database)
										.table(_this.tableName(model));
									const keys = Object.keys(
										index.fields || index.keys
									).map(key => _this.getRow(model, key));
									query = query.indexCreate(
										index.name,
										keys.length === 1 ? keys[0] : keys,
										index.options
									);
									query.run(_this.db, indexCallback);
								},
								modelCallback
							);
						});
				},
				cb
			);
		});
	}

	create(model, data, callback) {
		const idValue = this.getIdValue(model, data);
		const idName = this.idName(model);

		if (idValue === null || idValue === undefined) {
			delete data[idName];
		} else {
			data.id = idValue; // Set it to _id
			idName !== 'id' && delete data[idName];
		}

		if (process.env.TESTING) {
			data._createdAt = new Date();
		}

		this.save(model, data, null, callback, true);
	}

	update(
		model,
		where,
		data,
		callback) {
		const _this = this;


		let promise = r.db(_this.database).table(_this.tableName(model));
		if (where !== undefined) {
			promise = this.buildWhere(model, where, promise);
		}

		if (promise === null) {
			return callback && callback(null, { count: 0 });
		}

		Object.keys(data).forEach(k => {
			if (data[k].$add) {
				data[k] = r.row(k).add(data[k].$add);
			}
		});

		promise = promise.update(data, { returnChanges: true });

		return this
			.run(promise)
			.then(result => ({ count: result ? result.replaced : null }))
			.asCallback(callback);
	}

	updateAll(...args) {
		return this.update(...args);
	}

	updateOrCreate(model, data, callback) {
		const idValue = this.getIdValue(model, data);
		const idName = this.idName(model);

		if (idValue === null || idValue === undefined) {
			delete data[idName];
		} else {
			data.id = idValue; // Set it to _id
			idName !== 'id' && delete data[idName];
		}

		this.save(model, data, null, callback, false, true);
	}

	save(model, data, options, callback, strict, returnObject) {
		const _this = this;

		const save = client => {
			let idValue = _this.getIdValue(model, data);
			const idName = _this.idName(model);

			if (typeof strict === 'undefined') {
				strict = false;
			}

			Object.keys(data).forEach(key => {
				if (data[key] === undefined) {
					data[key] = null;
				}
			});

			r.db(_this.database)
				.table(_this.tableName(model))
				.insert(data, {
					conflict: strict ? 'error' : 'update',
					returnChanges: true
				})
				.run(client, (err, m) => {
					err = err || (m.first_error && new Error(m.first_error));
					if (err) {
						callback && callback(err);
					} else {
						const info = {};
						let object = null;

						if (m.inserted > 0) {
							// create
							info.isNewInstance = true;
							idValue = m.changes[0].new_val.id;
						}
						if (m.changes && m.changes.length > 0) {
							// update
							object = m.changes[0].new_val;
							_this.setIdValue(model, object, idValue);
							idName !== 'id' && delete object._id;
						}

						if (returnObject && m.changes && m.changes.length > 0) {
							callback && callback(null, object, info);
						} else {
							callback && callback(null, idValue, info);
						}
					}
				});
		};

		return this.getConnectionInstance().then(save);
	}

	exists(model, id, callback) {
		const _this = this;

		const exists = client => {
			r.db(_this.database)
				.table(_this.tableName(model))
				.get(id)
				.run(client, (err, data) => {
					callback(err, !!(!err && data));
				});
		};

		this.getConnectionInstance().then(exists);
	}

	find(model, id, options, callback) {
		const _this = this;
		let _keys;

		const find = client => {
			const idName = this.idName(model);

			let promise = r.db(_this.database).table(_this.tableName(model));

			if (idName == 'id') {
				promise = promise.get(id);
			} else {
				promise = promise.filter({ idName: id });
			}

			const rQuery = promise.toString();

			promise.run(client, (error, data) => {
				// Acquire the keys for this model
				_keys = _this._models[model].properties;

				if (data) {
					// Pass to expansion helper
					_expandResult(data, _keys);
				}

				// Done
				callback && callback(error, data, rQuery);
			});
		};

		return this.getConnectionInstance().then(find);
	}

	destroy(model, id, callback) {
		const _this = this;

		const destroy = client => r.db(_this.database)
			.table(_this.tableName(model))
			.get(id)
			.delete()
			.run(client, (error, result) => {
				callback(error);
			});

		return this.getConnectionInstance().then(destroy);
	}

	changeFeed(model, filter, options) {
		const _this = this;

		const createObservable = client => {
			if (!filter) {
				filter = {};
			}

			let promise = r.db(_this.database).table(_this.tableName(model));

			const idName = this.idName(model);

			if (filter.order) {
				let keys = filter.order;
				if (typeof keys === 'string') {
					keys = keys.split(',');
				}
				keys.forEach(key => {
					const m = key.match(/\s+(A|DE)SC$/);
					key = key.replace(/\s+(A|DE)SC$/, '').trim();
					if (m && m[1] === 'DE') {
						promise = promise.orderBy({ index: r.desc(key) });
					} else {
						promise = promise.orderBy({ index: r.asc(key) });
					}
				});
			} else {
				// default sort by id
				promise = promise.orderBy({ index: r.asc('id') });
			}

			if (filter.where) {
				if (filter.where[idName]) {
					const id = filter.where[idName];
					delete filter.where[idName];
					filter.where.id = id;
				}
				promise = this.buildWhere(model, filter.where, promise);
				/* if (promise === null)
				 return callback && callback(null, []) */
			}

			if (!_.isEmpty(filter.fields)) {
				promise = _this.buildPluck(model, filter.fields, promise);
			}

			if (filter.limit) {
				promise = promise.limit(filter.limit);
			}

			const defaultOptions = {
				changesOptions: {
					includeStates: true
				}
			};

			const feedOptions = _.defaultsDeep({}, defaultOptions, options);

			const rQuery = promise.toString();

			// console.log(rQuery)

			const observable = Rx.Observable.create(observer => {
				let sendResults = () => {
					_this.all(model, filter, options, (error, data) => {
						if (error) {
							return observer.onError(error);
						}

						observer.onNext(data);
					});
				};

				if (_.isNumber(feedOptions.throttle)) {
					sendResults = _.throttle(sendResults, feedOptions.throttle);
				}

				sendResults();

				let feed;
				promise = promise.changes(feedOptions.changesOptions);
				if (filter.skip) {
					promise = promise.skip(filter.skip);
				} else if (filter.offset) {
					promise = promise.skip(filter.offset);
				}

				promise
					.run(client)
					.then(res => {
						feed = res;
						let isReady = false;
						return feed.eachAsync(item => {
							if (item.state === 'ready') {
								isReady = true;
								sendResults();
							} else if (!item.state && isReady) {
								sendResults();
							}
						});
					})
					.catch(err => {
						observer.onError(err);
					});

				return () => {
					if (feed) {
						feed.close().catch(() => {});
					}
				};
			});

			return observable;
		};

		return this.getConnectionInstance().then(createObservable);
	}

	all(model, filter, options, callback) {
		const _this = this;
		const client = this.db;

		if (!filter) {
			filter = {};
		}

		let promise = r.db(_this.database).table(_this.tableName(model));

		const idName = this.idName(model);

		if (filter.order) {
			let keys = filter.order;
			if (typeof keys === 'string') {
				keys = keys.split(',');
			}
			keys.forEach(key => {
				const m = key.match(/\s+(A|DE)SC$/);
				key = key.replace(/\s+(A|DE)SC$/, '').trim();
				if (m && m[1] === 'DE') {
					promise = promise.orderBy(r.desc(key));
				} else {
					promise = promise.orderBy(r.asc(key));
				}
			});
		} else {
			// default sort by id
			if (process.env.TESTING) {
				promise = promise.orderBy(r.asc('_createdAt'));
			} else {
				promise = promise.orderBy({ index: r.asc('id') });
			}
		}

		if (filter.where) {
			if (filter.where[idName]) {
				const id = filter.where[idName];
				delete filter.where[idName];
				filter.where.id = id;
			}
			promise = this.buildWhere(model, filter.where, promise);
			if (promise === null) {
				return callback && callback(null, []);
			}
		}

		if (filter.skip) {
			promise = promise.skip(filter.skip);
		} else if (filter.offset) {
			promise = promise.skip(filter.offset);
		}

		if (!_.isEmpty(filter.fields)) {
			promise = _this.buildPluck(model, filter.fields, promise);
		}

		if (filter.limit) {
			promise = promise.limit(filter.limit);
		}

		const rQuery = promise.toString();

		// console.log(rQuery)

		return this.run(promise).then(data => {
			const _keys = _this._models[model].properties;
			const _model = _this._models[model].model;

			data.forEach((element, index) => {
				if (element.id && idName !== 'id') {
					element[idName] = element.id;
					delete element.id;
				}
				_expandResult(element, _keys);
			});

			if (process.env.TESTING) {
				data = data.map(item => {
					delete item._createdAt;
					return item;
				});
			}

			if (filter && filter.include) {
				_this._models[
					model
					].model.include(data, filter.include, (err, data) => {
					callback(err, data);
				});
			} else {
				callback && callback(null, data, rQuery);
			}
		});
	}

	destroyAll(model, where, callback) {
		const _this = this;
		const destroyAll = client => {
			if (!callback && typeof where === 'function') {
				callback = where;
				where = undefined;
			}

			let promise = r.db(_this.database).table(_this.tableName(model));
			if (where !== undefined) {
				promise = this.buildWhere(model, where, promise);
			}

			if (promise === null) {
				return callback(null, { count: 0 });
			}

			promise.delete().run(client, (error, result) => {
				callback(error, { count: result ? result.deleted : null });
			});
		};

		return this.getConnectionInstance().then(destroyAll);
	}

	count(model, where, options, callback) {
		const _this = this;

		callback = callback || (() => {});

		const count = client => {
			let promise = r.db(_this.database).table(_this.tableName(model));

			if (where && typeof where === 'object') {
				promise = this.buildWhere(model, where, promise);
			}

			if (promise === null) {
				return callback(null, 0);
			}

			promise.count().run(client, (err, count) => {
				callback(err, count);
			});
		};

		return this.getConnectionInstance().then(count);
	}

	updateAttributes(model, id, data, cb) {
		const _this = this;

		const updateAttributes = client => {
			Object.keys(data).forEach(key => {
				if (data[key] === undefined) {
					data[key] = null;
				}
			});
			r.db(_this.database)
				.table(_this.tableName(model))
				.get(id)
				.update(data)
				.run(client, (err, object) => {
					cb(err, data);
				});
		};

		return this.getConnectionInstance().then(updateAttributes);
	}

	disconnect() {
		this.db.close();
		this.db = null;
	}

	buildWhere(model, where, promise) {
		const _this = this;
		if (where === null || typeof where !== 'object') {
			return promise;
		}

		const query = this.buildFilter(where, model);

		if (query === undefined) {
			return promise;
		} else if (query === null) {
			return null;
		}
		return promise.filter(query);
	}

	buildFilter(where, model) {
		const filter = [];
		const _this = this;

		Object.keys(where).forEach(k => {
			// determine if k is field name or condition name
			const conditions = [
				'and',
				'or',
				'between',
				'gt',
				'lt',
				'gte',
				'lte',
				'inq',
				'nin',
				'near',
				'neq',
				'like',
				'nlike',
				'regexp'
			];
			const condition = where[k];

			if (k === 'and' || k === 'or') {
				if (_.isArray(condition)) {
					const query = _.map(condition, c => _this.buildFilter(c, model));

					if (k === 'and') {
						filter.push(_.reduce(query, (s, f) => s.and(f)));
					} else {
						filter.push(_.reduce(query, (s, f) => s.or(f)));
					}
				}
			} else if (
				_.isObject(condition) &&
				_.intersection(_.keys(condition), conditions).length > 0
			) {
				// k is condition
				_.keys(condition).forEach(operator => {
					if (conditions.includes(operator)) {
						// filter.push(operators[operator](k, condition[operator], _this.getPropertyDefinition.bind(_this, model)))
						filter.push(
							_this.getCondition(
								model,
								k,
								null,
								null,
								condition[operator],
								operator
							)
						);
					}
				});
			} else {
				// k is field equality
				filter.push(_this.getCondition(model, k, null, null, condition));
			}
		});

		if (_.findIndex(filter, item => item === null) >= 0) {
			return null;
		}

		if (filter.length == 0) {
			return undefined;
		}

		return _.reduce(filter, (s, f) => s.and(f));
	}

	buildPluck(model, fields, promise) {
		const pluckObj = fields.reduce(
			(acc, fieldName) => {
				_.set(acc, fieldName, true);
				return acc;
			},
			{}
		);

		return promise.pluck(pluckObj);
	}

	// Handle nested properties
	getCondition(model, key, relativePath, partialPath, criteria, operator, row) {
		const _this = this;

		row = this.getRow(
			model,
			key,
			relativePath,
			partialPath,
			criteria,
			operator,
			row
		);
		if (row.toString().includes('contains(')) return row;

		return _this.applyOperator(row, operator, criteria);
	}

	applyOperator(row, operator, criteria) {
		const operators = {
			between(row, value) {
				return row.gt(value[0]).and(row.lt(value[1]));
			},
			gt(row, value) {
				if (value === null || value === undefined) return null;
				return row.gt(value);
			},
			lt(row, value) {
				if (value === null || value === undefined) return null;
				return row.lt(value);
			},
			gte(row, value) {
				if (value === null || value === undefined) return null;
				return row.ge(value);
			},
			lte(row, value) {
				if (value === null || value === undefined) return null;
				return row.le(value);
			},
			inq(row, value) {
				const query = [];

				value.forEach(v => {
					query.push(row.eq(v));
				});

				const condition = _.reduce(query, (sum, qq) => sum.or(qq));

				return condition;
			},
			nin(row, value) {
				const query = [];

				value.forEach(v => {
					query.push(row.ne(v));
				});

				const condition = _.reduce(query, (sum, qq) => sum.and(qq));

				return condition;
			},
			neq(row, value) {
				return row.ne(value);
			},
			like(row, value) {
				return row.match(value);
			},
			nlike(row, value) {
				return row.match(value).not();
			},
			regexp(row, value) {
				return row.match(_.trim(`${value}`, '//'));
			}
		};

		if (operators[operator]) {
			return operators[operator](row, criteria);
		}
		return row.eq(criteria);
	}

	getNestedPropertyDefinition(model, path) {
		const modelDefinition = this.getModelDefinition(model);
		return _.get(modelDefinition, `properties.${path}`);
	}

	// Handle nested properties
	getRow(
		model,
		key,
		relativePath,
		partialPath,
		criteria,
		operator,
		row,
		isNested) {
		const _this = this;
		const props = relativePath || key.split('.');
		partialPath = partialPath || [];

		return props.reduce(
			(row, prop, index) => {
				partialPath.push(prop);
				const propDef = _this.getNestedPropertyDefinition(
					model,
					partialPath.join('.')
				) || {};

				if (Array.isArray(propDef) || Array.isArray(propDef.type)) {
					const _relativePath = props.slice(index + 1);
					return row(prop).contains(doc =>
						_this.getCondition(
							model,
							key,
							_relativePath,
							partialPath,
							criteria,
							operator,
							doc
						));
				}
				if (row.toString().includes('contains(')) return row;
				return row(prop);
			},
			row || this.r.row
		);
	}
}

/*
 Some values may require post-processing. Do that here.
 */
function _expandResult(result, keys) {
	Object.keys(result).forEach(key => {
		if (!keys.hasOwnProperty(key)) return;

		if (
			keys[key].type &&
			keys[key].type.name === 'Date' &&
			!(result[key] instanceof Date)
		) {
			// Expand date result data, backward compatible
			result[key] = moment.unix(result[key]).toDate();
		}
	});
}

function _hasIndex(_this, model, key) {
	// Primary key always hasIndex
	if (key === 'id') return true;

	const modelDef = _this._models[model];
	return (_.isObject(modelDef.properties[key]) &&
		modelDef.properties[key].index) ||
		(_.isObject(modelDef.settings[key]) && modelDef.settings[key].index);
}

function _toMatchExpr(regexp) {
	let expr = regexp.toString();
	const exprStop = expr.lastIndexOf('/');
	const exprCi = expr.slice(exprStop).search('i');

	expr = expr.slice(1, exprStop);

	if (exprCi > -1) {
		expr = `(?i)${expr}`;
	}

	return expr;
}

function _matchFn(k, cond) {
	const matchExpr = _toMatchExpr(cond);
	return row => row(k).match(matchExpr);
}

function _inqFn(k, cond) {
	return row => r.expr(cond).contains(row(k));
}
