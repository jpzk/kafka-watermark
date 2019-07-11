const KafkaJS = require("kafkajs");
const registry = require("avro-schema-registry")("http://localhost:8081");
const LZ4 = require("kafkajs-lz4");

const consumerId = consumerId;
const brokers = "172.31.0.16:9092";
const topic = topic

const kafka = new KafkaJS.Kafka({
  clientId: consumerId,
  brokers: brokers.split(",")
});

const resetOffsets = async () => {
  const admin = kafka.admin();
  await admin.connect();
  await admin.setOffsets({
    groupId: consumerId,
    topic: topic,
    partitions: [{
      partition: 0,
      offset: "0",
  }]
  })
  await admin.disconnect();
};

resetOffsets().then(() => {
  console.log("Offset reset to 0")

const consumer = kafka.consumer({ groupId: consumerId });
consumer.subscribe({ topic: topic });

KafkaJS.CompressionCodecs[KafkaJS.CompressionTypes.LZ4] = new LZ4().codec;

var watermark = -1;

function isValid(watermark, currentBlockNumber) {
  if (watermark < currentBlockNumber) return true;
  else return false;
}

consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    try {
      const decodedData = await registry.decode(message.value);

      // get blockNumber
      const blockNumber = decodedData.blockNumber;

      // check if higher than watermark
      if (!isValid(watermark, blockNumber)) {
        console.log(
          `Found a duplicate at ${blockNumber}, watermark is ${watermark}`
        );
      }

      // set watermark
      watermark = blockNumber;

      // give output to user
      if (blockNumber % 100 === 0) {
        console.log("Processed until " + blockNumber);
      }
    } catch (e) {
      this.logger.error(`${topic}: ${e.message}`);
      this.errorMeter.mark(1);
    }
  }
});
})