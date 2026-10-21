declare module 'kafkajs-snappy' {
  import type { CompressionCodec } from 'kafkajs'
  const SnappyCodec: () => CompressionCodec
  export default SnappyCodec
}
