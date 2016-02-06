var MongoClient = require('mongodb').MongoClient;
var async = require('async');
var Config = require('./config.js');

async.auto({

	db: function(callback){
		MongoClient.connect(Config.mongoUrl, function (err, db) {
			if (err) {
				callback(err);
			}
			console.log("Connected to", db.s.databaseName);
			callback(null, db);
		});
	},

	layerCollection: ['db', function(callback, results){
		results.db.collection(Config.layerCollectionName, function(err, col){
			if(err) callback(err);
			callback(null, col);
		});
	}],

	datasetCollection: ['db', function(callback, results){
		results.db.collection(Config.datasetCollectionName, function(err, col){
			if(err) callback(err);
			callback(null, col);
		});
	}],

	updateRasters: ['layerCollection', function(callback, results){
		var collection = results.layerCollection;
		var filter = {justVisualization: true};
		var setObject = {};
		setObject[Config.resultAttributeName] = Config.justVisValue;
		updateMany(collection, filter, setObject, callback);
	}],

	updateVectors: ['layerCollection', function(callback, results){
		var collection = results.layerCollection;
		var filter = {justVisualization: false};
		var setObject = {};
		setObject[Config.resultAttributeName] = Config.notJustVisValue;
		updateMany(collection, filter, setObject, callback);
	}],

	auids: ['updateVectors', 'datasetCollection', function(callback, results){
		var auIDs = [];
		var datasetCursor = results.datasetCollection.find();
		datasetCursor.each(function(err, item){
			if(item == null) {
				return callback(null, auIDs);
			}
			if (item.hasOwnProperty("featureLayers")) {
				Array.prototype.push.apply(auIDs, item.featureLayers);
			}
		});
	}],

	updateAU: ['updateVectors','auids', function(callback, results){
		var collection = results.layerCollection;
		var filter = {_id: {$in: results.auids}};
		var setObject = {};
		setObject[Config.resultAttributeName] = Config.auValue;
		updateMany(collection, filter, setObject, callback);
	}],

	close: ['updateVectors', 'updateRasters', 'updateAU', function(callback, results){
		results.db.close(false, function(err, result){
			if(err) callback(err);
			console.log("Disconnected");
			callback(null, result);
		});
	}]

});

var updateMany = function(collection, filter, setObject, callback){
	collection.updateMany(filter, {$set: setObject}, function(err, result){
		if(err) callback(err);
		console.log("UPDATE");
		console.log("  collection:", collection.s.name);
		console.log("  filter:    ", filter);
		console.log("  set object:", setObject);
		console.log("  result:    ", result.result);
		callback(null, result.result);
	});
};

