/*
1. input a currency exchange job
2. get the exchange rate and reput the job to the tube and delay with 60s
3. save 10 succesful exchange rate results to mongodb include the timstamp, then the job is done
4. if there is any problem reput to the tube and delay by 3 seconds
5. if failed more than 3 times bury the job

Stop the task if succeded 10 times or failed 3 times

*/

'use strict';
let $http = require('request-promise-json');
let bs = require('nodestalker');
let mongoose = require('mongoose');
let BBPromise = require('bluebird');
BBPromise.promisifyAll(mongoose);
BBPromise.promisifyAll(bs);
let co = require('co');
let config = require('./config.js');
let args = process.argv.slice(2);
let failed_attempts = 0;
let successful_attempts = 0;
let client = new bs.Client(config.bs.host + ':' + config.bs.port);

let job = {
	type: 'exchange',
	payload: {
		'from': 'USD',
		'to': 'HKD'
	}
};

let rateSchema = mongoose.Schema({
	'from': String,
	'to': String,
	'created_at': Date,
	'rate': Number
});

let rate = mongoose.model('rate', rateSchema);

mongoose.connect('mongodb://' + config.mongo.user + ':' + config.mongo.password +
	'@' + config.mongo.host + ':' + config.mongo.port + '/' + config.mongo.db);
let db = mongoose.connection;
db.on('error', console.error.bind(console, 'db connection error:'));

/**
	* Parse the command line arguments
	* @param {string} argument - a command line option
	*/

function parseArgs(argument) {
	if (argument === '-s') {
		seed();
	} else if (argument === '-w') {
		db.onceAsync('open')
		.then(function () {
			console.log('connected to DB');
			worker();
		});
	}
}

/**
	* Get the exchange rate
	* @param {string} source - the currency you are converting from
	* @param {string} currency - the currency you are converting to
	*/

function* exchange(source, currency) {
	let exchange_rate = yield $http.get('http://apilayer.net/api/live?access_key=' +
		config.keys.currency_layer + '&currencies=' + currency + '&source=' +
		source + '&format=1');
	return exchange_rate.quotes[source + currency].toFixed(2);
}

// the seed function puts the first job in the tube
function seed() {
	// use the right tube and put jobs in the queue
	client.use(config.bs.tube).onSuccess(function (tube) {
		console.log(tube);
		client.put(JSON.stringify(job), 0, 0, 0).onSuccess(function (job_id) {
			console.log(job_id);
		});
	});
}
// the worker function
function worker() {
	console.log('worker started');
	// watch a specified tube
	client.watch(config.bs.tube);
	// reserve a job
	client.reserve().onSuccess(function (current_job) {
		// convert the job to JSON object
		current_job.data = JSON.parse(current_job.data);
		console.log(current_job);
		if (current_job.data.type === 'exchange') {
			console.log('getting exchange rate');
			// get the exchange rate
			co(exchange(current_job.data.payload.from, current_job.data.payload.to))
			.then(function (current_rate) {
				// on success save the exchange_rate to db
				console.log(current_rate);
				let exchange_rate = new rate({
					from: current_job.data.payload.from,
					to: current_job.data.payload.to,
					created_at: new Date(),
					rate: current_rate
				});
				// save the exchange rate to db
				exchange_rate.save();
				successful_attempts++;
				if (successful_attempts < 10) {
					// put the job back into beanstalk after 60 seconds
					setTimeout(function () {
						client.put(JSON.stringify(job)).onSuccess(function (job_id) {
							console.log(job_id);
							worker();
						});
					}, 1000 * 60);
				}
			}, function (err) {
				console.log('job failed', err);
				failed_attempts++;
				if (failed_attempts < 3) {
					// put the job back into beanstalk after 3 seconds
					setTimeout(function () {
						client.put(JSON.stringify(job)).onSuccess(function (job_id) {
							console.log(job_id);
							worker();
						});
					}, 1000 * 3);
				}
			})
			.catch(function (err) {
				console.log(err);
			});
		}
	});
}

// parse the command line options
args.forEach(parseArgs);
