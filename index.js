const KafkaJS = require("kafkajs");
const LZ4 = require("kafkajs-lz4");

const argv = process.argv
const topic = argv[2];
const consumerId = argv[3];
const brokers = argv[4];
const field = argv[6]

const registry = require("avro-schema-registry")(argv[5]);

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

function isValid(watermark, current) {
  if (watermark < current) return true;
  else return false;
}

function isStrictIncrease(watermark, current) {
  if (watermark + 1 === current) {
    return true 
  } else return false
}

consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    try {
      const decodedData = await registry.decode(message.value);

      // get blockNumber
      const current = decodedData[field];

      // check if higher than watermark
      if (!isValid(watermark, current)) {
        console.log(
          `Found a duplicate at ${current}, watermark is ${watermark}`
        );
        process.exit()
      }

      if (!isStrictIncrease(watermark, current)) {
        console.log(
          `Found a missing record between ${watermark} - ${current}`)
        process.exit()  
      }

      // set watermark
      watermark = current;

      // give output to user
      if (current % 100 === 0) {
        console.log("Processed until " + current);
      }
    } catch (e) {
      console.log(e)
  }}
});
})