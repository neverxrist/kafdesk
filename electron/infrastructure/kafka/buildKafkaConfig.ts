import { logLevel, type KafkaConfig } from 'kafkajs'
import { authService } from '../../auth/authService'
import type { ClusterConfig, ConnectionAuthMode } from '../../types'

/** Build KafkaJS client config from a saved cluster connection. */
export function buildKafkaConfig(config: ClusterConfig): KafkaConfig {
  const kafkaConfig: KafkaConfig = {
    clientId: config.clientId || 'kafdesk',
    brokers: config.brokers,
    ssl: config.ssl || false,
    logLevel: logLevel.ERROR,
    connectionTimeout: 10_000,
    requestTimeout: 30_000,
  }

  const authMode = clusterAuthMode(config)

  if (authMode === 'oauthbearer') {
    kafkaConfig.sasl = {
      mechanism: 'oauthbearer',
      oauthBearerProvider: async () => {
        const token = await authService.getAccessToken(config.sso)
        if (!token) throw new Error('SSO token missing — login via Keycloak first')
        return { value: token }
      },
    }
  } else if (authMode === 'sasl' && config.sasl?.username) {
    const { mechanism, username, password } = config.sasl
    if (mechanism === 'scram-sha-256') {
      kafkaConfig.sasl = { mechanism: 'scram-sha-256', username, password }
    } else if (mechanism === 'scram-sha-512') {
      kafkaConfig.sasl = { mechanism: 'scram-sha-512', username, password }
    } else {
      kafkaConfig.sasl = { mechanism: 'plain', username, password }
    }
  }

  return kafkaConfig
}

export function clusterAuthMode(config: ClusterConfig): ConnectionAuthMode {
  return config.authMode || (config.sasl?.username ? 'sasl' : 'none')
}
