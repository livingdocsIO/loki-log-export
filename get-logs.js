const mergeStreams = require('./merge-logs')
const delay = require('util').promisify(setTimeout)

module.exports = function follow (opts) {
  const loki = require('axios').create({baseURL: opts.baseURL || 'http://localhost:3100'})
  const afterMs = opts.start.getTime ? opts.start.getTime() : Date.parse(opts.start)
  const endMs = opts.end.getTime ? opts.end.getTime() : Date.parse(opts.end)
  const query = opts.query || '{service="bluewin-test/varnish"}'

  return {
    [Symbol.asyncIterator]() {
      let after = `${afterMs}000000`
      const end = `${endMs}000000`
      return {
        async next() {
          let tries = 3
          while (tries--) {
            try {
              opts.log.debug(`Fetch after ${after}, end ${end}`)
              const res = await loki({
                url: '/loki/api/v1/query_range',
                params: {
                  limit: 5000,
                  direction: 'forward',
                  query,
                  start: after,
                  end
                }
              })

              if (res.data.status !== 'success') {
                throw new Error(`Invalid Loki Result: ${JSON.stringify(res.data)}`)
              }

              const logs = mergeStreams(res.data.data.result)

              if (logs && logs[0] && logs[0].ts === after) logs.shift()
              if (!logs.length) return {done: true}

              after = logs[logs.length - 1].ts

              return {done: false, value: logs}
            } catch (err) {
              opts.log.error({err}, 'Failed to fetch logs from loki')
            }
            await delay(1000)
          }
          throw new Error(`Failed to fetch logs after 3 retries.`)
        }
      }
    }
  }
}
