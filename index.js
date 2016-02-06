var MongoClient = require('mongodb').MongoClient;
var async = require('async');

var url = 'mongodb://localhost:27017/test';
var layerCollectionName = "areatemplate2";
var datasetCollectionName = "dataset";
var values = {
	auValue: "au",
	justVisValue: "raster",
	notJustVisValue: "vector"
};
var resultAttributeName = "layerType";


async.auto({


	db: function(callback){
		MongoClient.connect(url, function (err, db) {
			if (err) {
				callback(err);
			}
			console.log("Connected to", db.s.databaseName);

			//run(db, layerCollectionName, datasetCollectionName, values, resultAttributeName);
			callback(null, db);
		});
	},


	layerCollection: ['db', function(callback, results){
		results.db.collection(layerCollectionName, function(err, col){
			if(err) callback(err);
			callback(null, col);
		});
	}],

	datasetCollection: ['db', function(callback, results){
		results.db.collection(datasetCollectionName, function(err, col){
			if(err) callback(err);
			callback(null, col);
		});
	}],

	updateRasters: ['layerCollection', function(callback, results){
		var collection = results.layerCollection;
		var filter = {justVisualization: true};
		var setObject = {};
		setObject[resultAttributeName] = values.justVisValue;
		updateMany(collection, filter, setObject, callback);
	}],

	updateVectors: ['layerCollection', function(callback, results){
		var collection = results.layerCollection;
		var filter = {justVisualization: false};
		var setObject = {};
		setObject[resultAttributeName] = values.notJustVisValue;
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
				//console.log("IDs: ", item.featureLayers);
				Array.prototype.push.apply(auIDs, item.featureLayers);
			}
		});
	}],


	updateAU: ['updateVectors','auids', function(callback, results){
		var collection = results.layerCollection;
		var filter = {_id: {$in: results.auids}};
		var setObject = {};
		setObject[resultAttributeName] = values.auValue;
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

