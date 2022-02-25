#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <librdkafka/rdkafka.h>
#include <gst/gst.h>
#include <time.h>

char errstr[512];
const char *topic = "test";
rd_kafka_conf_t *conf = NULL;
rd_kafka_topic_conf_t *topic_conf = NULL;
rd_kafka_t *rk = NULL;
rd_kafka_topic_t *rkt = NULL;
unsigned int cnt = 0;

typedef struct _Payload {
	char payload[100];
	uint32_t timestamp;
} Payload;

static void on_delivery(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
	if (rkmessage->err) {
		fprintf(stderr, "%% Message delivery failed: %s\n",
				rd_kafka_message_errstr(rkmessage));
		printf("ERR!\n");
	} else {
		printf("Partition : %d\n", rkmessage->partition);
	}
}

void produce_msg(int cnt_in) {
	struct timespec tp;
	timespec_get(&tp, TIME_UTC);
	guint64 time = tp.tv_sec * 1000 + tp.tv_nsec/1000000;
	printf("%ld\n", time);

	char payload[100];
	char number[20];
	char timestamp[32];
	strcpy(payload, "HELLO FROM RO_KAFKA");
	strcat(payload, "/");
	sprintf(number, "%d", cnt);
	sprintf(timestamp, "%ld", time);
	strcat(payload, number);
	strcat(payload, "/");
	strcat(payload, timestamp);
	int len = strlen(payload);

	printf("PAYLOAD %s LEN %d\n", payload, len);

	rd_kafka_resp_err_t err = -12345;

	//async
	for (int j = 0; j < cnt_in; j++) {
		for (int i = 0; i < 15; i++) {
			if (rd_kafka_produce(rkt, i,
						RD_KAFKA_MSG_F_COPY,
						payload,
						len,
						NULL,
						0,
						&err) == -1) {
				printf("Err\n");
			}
		}
	}

	// for get return message 
	while (rd_kafka_outq_len(rk) > 0) rd_kafka_poll(rk, 0);
	//while (err == -12345) rd_kafka_poll(rk, 1000);
}

void init_kafka() {
	conf = rd_kafka_conf_new();
	topic_conf = rd_kafka_topic_conf_new();

	rd_kafka_conf_set_dr_msg_cb(conf, on_delivery);

	rd_kafka_conf_set(conf, "queue.buffering.max.ms", "1", errstr,  sizeof(errstr));
	rd_kafka_conf_set(conf, "socket.blocking.max.ms", "1", errstr,  sizeof(errstr));

	if (rd_kafka_conf_set(conf, "client.id", "c_producer", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		printf("%s\n", errstr);
		exit(1);
	}

	if (rd_kafka_conf_set(conf, "bootstrap.servers", "localhost:9092", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		printf("%s\n", errstr);
		exit(1);
	}


	if (rd_kafka_topic_conf_set(topic_conf, "acks", "all", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		printf("%s\n", errstr);
		exit(1);
	}

	/* Create Kafka producer handle */
	if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr)))) {
		printf("Failed to create new producer: %s\n", errstr);
		exit(1);
	}

	rkt = rd_kafka_topic_new(rk, "test", topic_conf);
}

GstPadProbeReturn fakesink_callback(GstPad *fakesink, GstPadProbeInfo *buffer, gpointer data) {
	g_print("#### SEND %d ####\n", cnt);
	cnt++;
	produce_msg(1);
	return GST_PAD_PROBE_OK;
}

int main(int argc, char *argv[]) {
	init_kafka();
	gst_init(&argc, &argv);
	GMainLoop *loop = g_main_loop_new(NULL, FALSE);

	//const char *pipeline_str = "videotestsrc ! decodebin ! videorate ! video/x-raw,framerate=1/20 ! videoconvert ! fakesink name=f_sink";
	const char *pipeline_str = "videotestsrc ! decodebin ! videorate ! video/x-raw,framerate=1/10 ! videoconvert ! fakesink name=f_sink";
	GstElement *pipeline = gst_parse_launch(pipeline_str, NULL);
	GstElement *fakesink = gst_bin_get_by_name(GST_BIN(pipeline), "f_sink");
	GstPad *sink = gst_element_get_static_pad(fakesink, "sink");
	gst_pad_add_probe(sink, GST_PAD_PROBE_TYPE_BUFFER, (GstPadProbeCallback) fakesink_callback, NULL, NULL);

	gst_element_set_state(pipeline, GST_STATE_PLAYING);

	g_main_loop_run(loop);

	return 0;
}
