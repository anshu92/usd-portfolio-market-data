# Data notice

The MIT license in this repository applies to the repository's original code and
documentation. It does not relicense third-party data.

The market-data build derives historical OHLCV and split-event records from the
[`defeatbeta/yahoo-finance-data`](https://huggingface.co/datasets/defeatbeta/yahoo-finance-data)
dataset. Its dataset card identifies Yahoo Finance chart data as the upstream source
and labels the database as Open Data Commons Attribution License 1.0 (ODC-By-1.0).
The source dataset, resolved revision, input hashes, and output hashes are recorded in
every release manifest.

The security universe is derived from the public Nasdaq Trader symbol-directory files
`nasdaqlisted.txt` and `otherlisted.txt`. Their source URLs, file creation timestamps,
and SHA-256 hashes are recorded in the universe metadata and release manifest.

Release assets are intended only for historical research and education. They are not a
live quote feed, do not represent executable prices, and do not claim that raw close is
an adjusted or total-return series. Database licensing may not cover every right in the
individual contents, trademarks, or upstream service terms; downstream users remain
responsible for determining whether their use and redistribution are permitted.
