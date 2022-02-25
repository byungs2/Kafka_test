from kafka import KafkaConsumer
from kafka import TopicPartition
from kafka import OffsetAndMetadata
import multiprocessing
import time

# TODO add camera_name key logic, now we're using random partition id as key
class DataProvider:
    def __init__(self, **args):
        self.provider = KafkaConsumer(group_id=args['group_id'],
                max_poll_records=args['number_of_max_record'],
                enable_auto_commit=args['auto_commit'],
                bootstrap_servers=args['boot_server']); # Kafka provider builder
        self.partition_flag = {}; # flag to check completeness of lockstep
        self.num_partitions = {}; # number of flags for each partitions
        self.cmpr = {}; # target of partition_flag
        self.tmp_data_list = []; # temporal data list
        self.current = time.time() * 1000; # criteria of lockstep
        self.next_stamp = None; # criteria of next lockstep
        self.tolerance = int((1.0/args['fps']) * 1000); # set how much error will be accepted as normal
        self.topics = None; # topic name list
        self.mask = 1; # mask for flags 

    def subscribe_topic(self, topics):
        self.provider.subscribe(topics);
        self.topics = topics;
        return self;

    def set_flags(self):
        for i in self.topics:
            lens = len(self.provider.partitions_for_topic(i));
            self.partition_flag[i] = 0x0000;
            self.num_partitions[i] = lens;
            self.cmpr[i] = (1 << lens) - 1;
        return self;

    def seek_starting_point(self, look_back):
        self.provider.poll();
        for i in self.topics:
            for j in range(self.num_partitions[i]):
                topic_partition = TopicPartition(i, j);
                current_commit = self.provider.committed(topic_partition);
                self.provider.seek(topic_partition, current_commit-look_back);

    def lock_steps(self, msg):
        self.current = 1645772841266 # only for test

        topic = msg.topic;
        offset = msg.offset;
        partition = msg.partition;
        timestamp = int(msg.value.split('/')[2]) \
                if len(msg.value.split('/')) \
                > 2 else msg.timestamp;
        topic_partition = TopicPartition(topic, partition);
        lower_bound = self.current - self.tolerance;
        upper_bound = self.current + self.tolerance;
        current_commit = self.provider.committed(topic_partition);
        if ((timestamp > lower_bound and timestamp < upper_bound) 
        and ((self.partition_flag[topic] >> partition) & 1) == 0):
            offset_meta = OffsetAndMetadata(offset=offset, metadata=None);
            self.partition_flag[topic] |= (self.mask << partition);
            self.provider.commit({topic_partition : offset_meta});
            self.provider.pause(topic_partition);
            self.tmp_data_list.append({partition : timestamp}); # shared memory test
            self.next_stamp = timestamp + self.tolerance; # set next lockstep with last timestamp
            print("### Pause ###", partition, offset, timestamp);
        else:
            if timestamp >= upper_bound:
                self.provider.seek(topic_partition, current_commit + 1);
                print("### Lockstep ###", partition, offset, timestamp, upper_bound);
            elif timestamp <= lower_bound:
                offset_meta = OffsetAndMetadata(offset=offset, metadata=None);
                self.provider.commit({topic_partition : offset_meta});
                print("### Pass ###", topic, partition, offset, timestamp, lower_bound);

    # TODO timeout, resume topic by topic
    # I assumed our case can be handled with single topic case
    # But for future, I leave the door open to expand 
    # into multiple topics case
    def march_steps(self, transportor):
        for i in self.topics:
            if (self.partition_flag[i] == self.cmpr[i]):
                self.partition_flag[i] = 0x0000;
                self.provider.resume(list(self.provider.paused())[0]); # resume all topics
                self.current = self.next_stamp;
                transportor.append(self.tmp_data_list);
                self.tmp_data_list = [];
                print("### March ###");

    def fetch_datas(self, transportor):
        for msg in self.provider:
            self.lock_steps(msg);
            self.march_steps(transportor);

# You can use this function as data consumer.
# Pls write your logics
def consume(data):
    while(True):
        if (len(data) > 0):
            print("### POP ###", data.pop(0));

if __name__ == '__main__':
    manager = multiprocessing.Manager();
    data_queue = manager.list();

    data_provider = DataProvider(group_id='cons1',
            number_of_max_record=1,
            auto_commit=False,
            boot_server='localhost:9092',
            fps=15.0);

    data_provider.subscribe_topic(['test'])\ # you can subscribe multiple topics, but for now, you cannot handle it properly
    .set_flags()\
    .seek_starting_point(100); # set steps by how much to look back 

    data_handler = multiprocessing.Process(target=data_provider.fetch_datas, args=[data_queue]);
    data_consumer = multiprocessing.Process(target=consume, args=[data_queue]);

    data_handler.start();
    data_consumer.start();
    data_handler.join();
    data_consumer.join();
