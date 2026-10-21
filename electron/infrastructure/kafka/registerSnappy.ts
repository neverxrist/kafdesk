import kafkajs from 'kafkajs'
import SnappyCodec from 'kafkajs-snappy'

/** KafkaJS core only ships GZIP; local stacks often produce Snappy. */
export function registerSnappyCodec(): void {
  kafkajs.CompressionCodecs[kafkajs.CompressionTypes.Snappy] = SnappyCodec
}

registerSnappyCodec()
