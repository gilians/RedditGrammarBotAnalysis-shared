# RedditGrammarBotAnalysis

A processing script for PySpark that can be used for the analysis of comments by
[/u/CommonMisspellingBot](https://reddit.com/u/CommonMisspellingBot) and adapted to perform similar
types of analysis on comments of other spell correction bots.

The script was written for the Managing Big Data course at the University of Twente in Enschede,
The Netherlands. Hence, due to the limited timeframe that was available, some of the choices made
in this script might require further explanation that is not available in this code, but is
available in the associated report.

## How to use
The script is assumed to be ran on a cluster with PySpark 2.4.7, though other versions of PySpark might
also work.

The script requires Parquet-files containing Reddit comments to do its job. Recommended is to use
converted Reddit comments datasets from
[Pushshift](https://files.pushshift.io/reddit/comments/).

For the individual assessing this script for the Managing Big Data course, any instance of
`sXXXXXXX` in `processing.py` shall be replaced with the student number of the publisher of this
repository, Gilian. This concerns line 12, 57 and 58. In addition, for the data to be written to
sensible paths, replace the paths at line 44 and line 50.
