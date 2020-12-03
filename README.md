# Loki log export to s3 buckets

```bash
export AWS_REGION=eu-central-1
export AWS_BUCKET=your-log-bucket
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export LOKI_HOST=http://localhost:3100

export EXTRACTORS='[{
  "prefix": "some-service/",
  "query": "{service=\"some-service\"}",
  "transform": "json"
},
{
  "prefix": "another-service/",
  "query": "{service=\"another-service\"}",
  "transform": "json"
}]'

docker run --name loki-log-export livingdocs/loki-log-export:1.0.0
```

Will run the log export every hour at 5 past and upload gzipped log files to s3:
```
some-service/2020/12/01/00.log.gz
some-service/2020/12/01/01.log.gz
...
some-service/2020/12/01/23.log.gz
some-service/2020/12/02/00.log.gz

another-service/2020/12/01/00.log.gz
another-service/2020/12/01/01.log.gz
...
another-service/2020/12/01/23.log.gz
another-service/2020/12/02/00.log.gz
```
