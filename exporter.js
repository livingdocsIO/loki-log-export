const assert = require('assert')
const {promises: {pipeline}, Transform} = require('stream')
const CronJob = require('cron').CronJob
const zlib = require('zlib')
const getLogs = require('./get-logs')
const s3WriteStream = require('s3-streams').WriteStream
const S3 = require('aws-sdk').S3

module.exports = function createExporter (opts) {
  const pino = require('pino')({base: null})

  const {awsBucket, awsRegion, awsAccessKeyId, awsSecretAccessKey, lokiHost} = opts
  assert(awsBucket, `The parameter 'opts.awsBucket' is required.`)
  assert(awsRegion, `The parameter 'opts.awsRegion' is required.`)
  assert(awsAccessKeyId, `The parameter 'opts.awsAccessKeyId' is required.`)
  assert(awsSecretAccessKey, `The parameter 'opts.awsSecretAccessKey' is required.`)
  assert(lokiHost, `The parameter 'opts.lokiHost' is required.`)

  const s3 = new S3({
    region: awsRegion,
    accessKeyId: awsAccessKeyId,
    secretAccessKey: awsSecretAccessKey
  })

  const createS3WriteStream = (key) => new s3WriteStream(s3, {Bucket: awsBucket, Key: key})

  function toDateKey (prefix, date) {
    return function (hour) {
      const y = date.getFullYear()
      const m = `${date.getMonth() + 1}`.padStart(2, 0)
      const d = `${date.getDate()}`.padStart(2, 0)
      const h = `${hour}`.padStart(2, 0)
      const start = new Date(date)
      start.setHours(hour, 0, 0, 0)

      const end = new Date(date)
      end.setHours(hour + 1, 0, 0, 0)

      return {
        key: `${y}-${m}-${d}/${h}.log.gz`.replace(/^\/?/, prefix || ''),
        start,
        end
      }
    }
  }

  const hoursOfDay = Object.freeze([
    0, 1, 2, 3, 4, 5, 6, 7, 8,
    9, 10, 11, 12, 13, 14, 15, 16,
    17, 18, 19, 20, 21, 22, 23
  ])

  async function getHoursToProcess (prefix) {
    const days = []
    const today = new Date()
    days.push(hoursOfDay.slice(0, today.getHours()).map(toDateKey(prefix, today)))

    // Preload x days
    for (let i = 1; i < 2; i++) {
      const pastDay = new Date()
      pastDay.setDate(today.getDate() - i)
      pastDay.setHours(0,0,0,0)
      days.push(hoursOfDay.map(toDateKey(prefix, pastDay)))
    }

    const keys = []
    for (const day of days) {
      if (!day.length) continue
      const {Contents} = await s3.listObjectsV2({
        Bucket: awsBucket,
        Prefix: day[0].key.replace(/..\.log\.gz$/, '')
      }).promise()
      keys.push(...Contents.map(({Key}) => Key))
    }

    const existing = new Set(keys)
    const toProcess = []
    for (const day of days) {
      for (const hour of day) {
        if (!existing.has(hour.key)) toProcess.push(hour)
      }
    }

    // Returns an array of objects with keys
    // {start: date, end: date, key: 'prefix/2020/11/01/00.log.gz'}
    // {start: date, end: date, key: 'prefix/2020/11/01/...log.gz'}
    // {start: date, end: date, key: 'prefix/2020/11/01/23.log.gz'}
    return toProcess
  }

  async function start (extractor) {
    assert(extractor.query, `The parameter 'extractor.query' is required.`)
    assert(extractor.transform, `The parameter 'extractor.transform' is required.`)
    if (extractor.transform === 'json') {
      extractor.transform = jsonTransform
    } else if (typeof extractor.transform === 'string') {
      extractor.transform = (new Function(`return ${extractor.transform}`))()
    }

    const hours = await getHoursToProcess(extractor.prefix)
    for (const hour of hours) {
      const now = Date.now()
      pino.info(`Processing logs for ${hour.key}`)
      await pipeline(
        getLogs({
          log: pino,
          baseURL: lokiHost,
          query: extractor.query,
          start: hour.start,
          end: hour.end
        }),
        logsToText(extractor),
        zlib.createGzip(),
        createS3WriteStream(hour.key)
      )
      pino.info(`Persisted logs for ${hour.key}. Took ${Date.now() - now}ms.`)
    }
  }

  function startCron (extractor) {
    const job = new CronJob('05 * * * *', () => start(extractor))
    return job.start()
  }

  function jsonTransform ({value}) {
    // Fix varnish user agents
    value = value.replace(/\\x[a-f0-9]{2}/g, '')
    JSON.parse(value)
    return value
  }

  function logsToText ({transform}) {
    return new Transform({
      objectMode: true,
      transform (lines, _, done) {
        let str = ''
        for (const line of lines) {
          try {
            const log = transform(line)
            if (log) str += `${log}\n`
          } catch (err) {
            pino.info({err}, `Parsing of line failed: ${line.value}`)
          }
        }
        done(null, str)
      }
    })
  }

  return {
    log: pino,
    start,
    startCron
  }
}
