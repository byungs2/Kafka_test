const kafka = require('kafka-node');
const Transform = require('stream').Transform;
const ConsumerGroupStream = kafka.ConsumerGroupStream;
const ProducerStream = kafka.ProducerStream;
const Consumer = kafka.Consumer;
const Offset = kafka.Offset;
const Client = kafka.KafkaClient;
const topic = 'test';
let user_offset = 100;
let client_offset = 0;

const client = new Client({ kafkaHost: 'localhost:9092'});
//const topics = [{ topic: topic, partition: 0, offset: client_offset}, { topic: topic, partition: 1, offset: client_offset }, { topic: topic, partition: 2, offset: 3}];
const topics = [{ topic: topic, partition: 0, offset: client_offset}];
const options = { autoCommit: false, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024*1024, fromOffset: true};

const consumer = new Consumer(client, topics, options);
const offset = new Offset(client);

/*
const messageTransform = new Transform({
	objectMode: true,
	decodeStrings: true,
	transform (message, encoding, callback) {
	console.log(`Received message ${message.value} transforming input`);
		callback(null, {
		topic: 'test',
		messages: `You have been (${message.value}) made an example of`
		});
	}
});

offset.fetchCommits('ExampleTestGroup', [{ topic: 'test', partition: 0, offset: 5},
	{ topic: 'test', partition: 1, offset: 5},
	{ topic: 'test', partition: 2, offset: 5}],
	function (err, data) {
		console.log(data);
	});

offset.fetch(topics, function (err, data) {
	console.log(data, "FETCH!!");
	console.log(err, "ERROR");
});

const cgs = new ConsumerGroupStream({
	kafkaHost: '127.0.0.1:9092',
	groupId: 'ExampleTestGroup',
	sessionTimeout: 15000,
	protocol: ['roundrobin'],
	asyncPush: false,
	id: 'consumer1',
	fromOffset: 'earliest',
	autoCommit: false
}, 'test');

cgs.on('readable', ()=> {
	while(null !== (chunk = cgs.read())) {
		console.log(chunk, "chunk");
	}
});
*/

consumer.on('message', function (message) {
	console.log(message.offset, message.value, 'msg 1');
	if (message.offset === 500) {
		consumer.close (function (res) {
			consumer.setOffset('test', 0, 0);
			console.log(message.offset);
			console.log('close');
			consumer.emit('close');
		});
	}
});

consumer.on('close', function (res) {
	console.log('close cb start');
	consumer.fetch();
	consumer.on('message', function (message) {
		console.log(message.offset, message.value, 'msg 1');
		if (message.offset === 1) {
			consumer.close (function (res) {
				console.log('close');
				consumer.emit('close');
			});
		}	
	});
});

consumer.on('error', function (err) {
	console.log('error', err);
});

