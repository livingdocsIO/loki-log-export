const extractors = JSON.parse(process.env.EXTRACTORS || '[]')
const exporter = require('./exporter')({
  awsBucket: process.env.AWS_BUCKET,
  awsRegion: process.env.AWS_REGION,
  awsAccessKeyId: process.env.AWS_ACCESS_KEY_ID,
  awsSecretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  lokiHost: process.env.LOKI_HOST || 'http://localhost:3100'
})

for (const extractor of extractors) {
  exporter.log.warn(`Processor started for ${extractor.prefix}: ${extractor.query}`)
  if (process.argv.includes('--once')) {
    exporter.start(extractor)
  } else {
    exporter.startCron(extractor)
  }
}

const prexit = require('prexit')
prexit.signals.push('uncaughtException', 'unhandledRejection')
prexit.logExceptions = false

prexit(async (signal, error) => {
  const uptime = Math.round(process.uptime() * 100) / 100
  if ([0, 'SIGTERM', 'SIGINT'].includes(signal)) {
    if (signal === 0) exporter.log.warn(`Shutting down after running for ${uptime}s`)
    else exporter.log.warn(`Signal ${signal} received. Shutting down after running for ${uptime}s`)
  } else {
    const err = signal instanceof Error ? signal : error
    exporter.log.fatal({err}, `Processing error. Shutting down after running for ${uptime}s`)
  }
})
