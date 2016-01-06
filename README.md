# MicroCast

Cooperative video streaming.

Reference: [MicroCast: cooperative video streaming on smartphones](http://dl.acm.org/citation.cfm?id=2307643)

## Requirements

- Python 3.0+
- [Python m3u8 parser](https://pypi.python.org/pypi/m3u8)

## Run m3u8 server

```bash
python -m http.server
```

## Run microdownload_master

```bash
python microdownload_master.py --host 59.66.123.114 --port 4000 --url "http://www.usdi.net.tw/video/hls/Taylor_W1_S1.ts.m3u8"
```

## Run microdownload_slave

```bash
python microdownload_slave.py --host 59.66.123.53 --port 4000 --store_dir "data"
```
