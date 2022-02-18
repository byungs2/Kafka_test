#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <librdkafka/rdkafka.h>

int main(int argc, char *argv[]) {
	char hostname[128];
	char errstr[512];
	const char *topic = "test2";

	rd_kafka_conf_t *conf = rd_kafka_conf_new();

	if (gethostname(hostname, sizeof(hostname))) {
		fprintf(stderr, "%% Failed to lookup hostname\n");
		exit(1);
	}

	if (rd_kafka_conf_set(conf, "client.id", hostname, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		fprintf(stderr, "%% %s\n", errstr);
		exit(1);
	}

	if (rd_kafka_conf_set(conf, "bootstrap.servers", "localhost:9092", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		fprintf(stderr, "%% %s\n", errstr);
		exit(1);
	}

	rd_kafka_topic_conf_t *topic_conf = rd_kafka_topic_conf_new();

	if (rd_kafka_topic_conf_set(topic_conf, "acks", "all", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		fprintf(stderr, "%% %s\n", errstr);
		exit(1);
	}

	/* Create Kafka producer handle */
	rd_kafka_t *rk;
	if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr)))) {
		fprintf(stderr, "%% Failed to create new producer: %s\n", errstr);
		exit(1);
	}

	rd_kafka_topic_t *rkt = rd_kafka_topic_new(rk, topic, topic_conf);

	const char *payload = "HELLO FROM RO_KAFKA\n";

	if (rd_kafka_produce(rkt, 0,
				RD_KAFKA_MSG_F_COPY,
				"HELLO", 
				strlen("HELLO"),
				NULL,
				NULL,
				NULL) == -1) {
		fprintf(stderr, "%% Failed to produce to topic %s: %s\n", topic, rd_kafka_err2str(rd_kafka_errno2err(errno)));
	}


	return 0;
}
