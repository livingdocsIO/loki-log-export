module.exports = mergeSort

// Merges multiple loki streams into one single array of logs
// transforms:
// [
//   {stream: {container_id: '123'}, values: [["1606521574140524000", "first"]]}
//   {stream: {container_id: '321'}, values: [["1606521574140524001", "second"]]}
// ]
// to a sorted array by ts:
// [
//   {ts: "1606521574140524000", value: "first", stream: {container_id: '123'}}
//   {ts: "1606521574140524001", value: "second", stream: {container_id: '321'}}
// ]
function mergeSort (streams, transform = (entry) => entry[1]) {
  if (!streams.length) return []

  const result = []

  let hasEntries = true
  while (hasEntries) {
    let lowest = 0
    for (let i = 0; i < streams.length; i++) {
      if (streams[i].values[0]?.[0] < (streams[lowest].values[0]?.[0] || Infinity)) lowest = i
    }

    if (!streams[lowest]) throw new Error(`Fatal error: ${JSON.stringify(streams)}`)

    const elem = streams[lowest].values.shift()
    if (!elem) hasEntries = false
    else result.push({ts: elem[0], value: elem[1], stream: streams[lowest].stream})
  }

  return result
}

// const data = require('./log.json')
// const result = mergeSort(data, ([ts, str], stream) => `${ts} - ${str}`)
// console.log(result[0], result[1],  result[result.length - 1])
