const kafka = require('kafka-node');
const Producer = kafka.Producer;
const KeyedMessage = kafka.KeyedMessage;
const client = new kafka.KafkaClient();
const producer = new Producer(client);
const km = new KeyedMessage('key', 'message');
const payloads = [ { topic: 'test', messages: 'a', partition: 0 }, { topic: 'test', messages: 'part2', partition: 1 }, { topic: 'test', messages: 'part3', partition: 2 }];

producer.on('ready', function () {
	producer.send(payloads, function (err, data) {
		console.log(data);
		console.log(err);
	});
});

