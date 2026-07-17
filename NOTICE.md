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

Raw company facts and filing metadata are derived from the U.S. Securities and
Exchange Commission's EDGAR bulk files and public APIs. Insider data is derived from
the SEC Insider Transactions Data Sets, and institutional holdings are derived from
the SEC Form 13F Data Sets. These records are presented as filed; SEC dataset
disclaimers state that filer-supplied data and extraction can contain inaccuracies and
are not substitutes for the full filings. The producer preserves accession numbers,
source URLs, retrieval times, source revisions, filing/acceptance dates, and reporting
lags. SEC source pages:

- <https://www.sec.gov/search-filings/edgar-application-programming-interfaces>
- <https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets>
- <https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets>

Short-interest records are derived from FINRA Equity Short Interest through FINRA's
official data API. They are periodic observations, not real-time short positions. The
producer preserves both settlement and official publication dates and makes rows
usable only on publication. FINRA source pages:

- <https://www.finra.org/finra-data/browse-catalog/equity-short-interest>
- <https://developer.finra.org/docs>

Form 13F does not provide a canonical ticker mapping. Reviewed CUSIP overrides take
priority; otherwise the producer permits only a unique exact normalized SEC registrant
name and marks that method on the row. Unmapped positions are omitted, mapped coverage
is therefore partial, and 13F observations must never be described as current
holdings.

Release assets are intended only for historical research and education. They are not a
live quote feed, do not represent executable prices, and do not claim that raw close is
an adjusted or total-return series. Database licensing may not cover every right in the
individual contents, trademarks, or upstream service terms; downstream users remain
responsible for determining whether their use and redistribution are permitted.
