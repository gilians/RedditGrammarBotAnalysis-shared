import time

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType

BOT_USERNAME = "CommonMisspellingBot"
SPELLING_REGEX = r"\*{2}(.*)\*{2} is actually spelled \*{2}(.*)\*{2}\. You"

timestr = time.strftime("%Y%m%d_%H%M%S")

with open('/home/sXXXXXXX/reddit/foreign_language_subreddits.txt') as f:
    prefix = "/r/"
    foreign_subreddits = [subreddit[len(prefix):].lower() for subreddit in f.read().splitlines()]


def remove_data_skew(large_df, small_df, col_name):
    """
    Performs key-salting on the provided dataframes to reduce data skewness for certain operations that require large
    amounts of data shuffling. For some operations, this reduces the chances of one or more tasks in a stage taking
    huge amounts of time due to data being very unevenly distributed over partitions.
    Inspired by https://www.youtube.com/watch?v=d41_X78ojCg.
    :param large_df: the largest dataframe in the join-operation
    :param small_df: the smallest dataframe in the join-operation
    :param col_name: the name of the column to be salted
    :return: A tuple of the modified largest dataframe and modified smallest dataframe.
    """
    df1 = large_df.withColumn("{}_salted".format(col_name),
                              F.concat(F.col(col_name), F.lit("_"), F.lit(F.floor(F.rand(seed=10000) * 10))))
    df2 = small_df.withColumn("exploded_col", F.explode(F.array([F.lit(x) for x in range(10)])))

    return df1, df2


def write_data(df, file_identifier):
    """
    Writes the dataframe to a parquet-file and a csv-file, prefixed with a timestamp.
    :param df: dataframe to write
    :param file_identifier: the identifier, prefixed with a timestamp, will form the filename that is used
    """
    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .save("/user/sXXXXXXX/reddit/processed/{}_{}_nf.parquet".format(timestr, file_identifier))

    df.write \
        .mode("overwrite") \
        .option("header", "true") \
        .format("csv") \
        .save("/user/sXXXXXXX/reddit/processed/{}_{}_nf.csv".format(timestr, file_identifier))


spark = SparkSession.builder.getOrCreate()

# Read relevant data and immediately remove all comments posted by "[deleted]", as these will skew the results.
data = spark.read \
    .parquet("/user/sXXXXXXX/reddit/RC_2019-01.parquet", "/user/sXXXXXXX/reddit/RC_2019-02.parquet",
             "/user/sXXXXXXX/reddit/RC_2019-03.parquet") \
    .select(F.col("id"), F.col("created_utc"), F.col("author"), F.col("edited"), F.col("body"), F.col("parent_id"),
            F.col("subreddit")) \
    .filter(F.col("author") != "[deleted]") \
    .where(~F.lower(F.col("subreddit")).isin(foreign_subreddits)) \
    .cache()

# Retrieve comments by bot and extract incorrect and correct spelling from text.
bot_comments = data \
    .filter(F.col("author") == BOT_USERNAME) \
    .select(F.col("created_utc"),
            F.regexp_extract("body", SPELLING_REGEX, 1).alias("incorrect_spelling"),
            F.regexp_extract("body", SPELLING_REGEX, 2).alias("correct_spelling"),
            F.regexp_extract("parent_id", r"t1_(.*)", 1).alias("parent_comment_id"),
            F.col("subreddit")) \
    .where((F.col("incorrect_spelling") != "") & (F.col("correct_spelling") != "")) \
    .cache()

# Retrieve number of comments corrected by CMB, and number of distinct subreddits in which comments were corrected.
bot_aggs = bot_comments \
    .select(F.countDistinct("subreddit").alias("distinct_subreddits"),
            F.countDistinct("parent_comment_id").alias("distinct_parent_comments"))

bot_aggs.show()

# Retrieve number of distinct comments and subreddits in which each combination of incorrect and correct spelling
# occurs.
bot_correction_aggs = bot_comments \
    .select(F.col("incorrect_spelling"), F.col("correct_spelling"), F.col("parent_comment_id"), F.col("subreddit")) \
    .groupBy(F.col("incorrect_spelling"), F.col("correct_spelling")) \
    .agg(F.countDistinct("parent_comment_id").alias("distinct_parent_comments"),
         F.countDistinct("subreddit").alias("distinct_subreddits")) \
    .orderBy(F.col("distinct_parent_comments"), ascending=False) \
    .cache()

bot_correction_aggs.show()
write_data(bot_correction_aggs, "bot_correction_aggs")
bot_correction_aggs.unpersist()

# Retrieve the number of corrections in each subreddit.
bot_sub_aggs = bot_comments \
    .select(F.col("subreddit"), F.col("parent_comment_id")) \
    .groupBy(F.col("subreddit")) \
    .agg(F.countDistinct("parent_comment_id").alias("distinct_parent_comments")) \
    .orderBy(F.col("distinct_parent_comments"), ascending=False) \
    .cache()

write_data(bot_sub_aggs, "bot_correction_subs")
bot_sub_aggs.show()
bot_sub_aggs.unpersist()

# Retrieve required fields for all comments
all_comments = data \
    .select(F.col("id").alias("comment_id"),
            F.col("created_utc"),
            F.col("author"),
            F.col("edited"),
            F.col("body"),
            F.col("subreddit"))

# Join CommonMisspellingBot (CMB) comments with parent comment and determine whether comment was corrected
# after CMB replied.
#
# Possibly misleading assumptions:
# - Assumes that grammar error was edited no earlier than at provided "edited" timestamp.
# - Assumes that edited comments always show as edited, which is not always the case (e.g. if edit was made
#   very shortly after the comment was posted).
# - Assumes that a correction is always performed by removing the incorrect and adding the correct spelling.
corrected_comments = bot_comments \
    .alias("bc") \
    .join(all_comments.alias("ac"), F.col("ac.comment_id") == F.col("bc.parent_comment_id")) \
    .select(F.col("ac.comment_id").alias("comment_id"),
            F.col("bc.created_utc").alias("corrected_utc"),
            F.col("ac.author").alias("author"),
            F.col("ac.subreddit"),
            F.col("bc.incorrect_spelling").alias("incorrect_spelling"),
            F.col("bc.correct_spelling").alias("correct_spelling"),
            F.col("ac.edited").alias("edited"),
            F.expr("ac.body rlike concat('(?i)', '\\\\b', incorrect_spelling, '\\\\b')").alias("incorrect_in_comment"),
            F.expr("ac.body rlike concat('(?i)', '\\\\b', correct_spelling, '\\\\b')").alias("correct_in_comment")) \
    .withColumn("corrected",
                (F.col("edited") != "false") & (F.col("edited").cast(LongType()) > F.col("corrected_utc")) & ~(
                    F.col("incorrect_in_comment")) & F.col("correct_in_comment")) \
    .cache()

bot_comments.unpersist()

# Retrieve the number of times a combination of incorrect and correct spelling occurred, the number of times the
# incorrect spelling was still in the original comment (OC), the number of times the correct spelling was in the
# OC, and the number of times the OC was presumably corrected.
corrected_comments_agg = corrected_comments \
    .groupBy(F.col("incorrect_spelling"), F.col("correct_spelling")) \
    .agg(F.countDistinct("comment_id").alias("total"),
         F.countDistinct(F.when(F.col("incorrect_in_comment"), F.col("comment_id"))).alias("with_incorrect"),
         F.countDistinct(F.when(F.col("correct_in_comment"), F.col("comment_id"))).alias("with_correct"),
         F.countDistinct(F.when(F.col("corrected"), F.col("comment_id"))).alias("corrected")) \
    .cache()

corrected_comments.unpersist()

write_data(corrected_comments_agg, "corrected_comments_agg")
corrected_comments_agg.show()
corrected_comments_agg.unpersist()

# Retrieve all combinations of (author, misspelling, correct spelling).
corrected_per_author = corrected_comments \
    .groupBy(["author", "incorrect_spelling", "correct_spelling"]) \
    .agg(F.count(F.col("comment_id")).alias("times_corrected"),
         F.min(F.col("corrected_utc")).alias("first_corrected_utc"))

# Retrieve, for each (author, misspelling, correct spelling) combination, all comments that were posted by the author
# after this misspelling was first corrected.
# groupBy(F.first(...)) is performed to remove duplicate rows created by salting.
all_comments, corrected_per_author = remove_data_skew(all_comments, corrected_per_author, "author")
comments_per_corrected_author = corrected_per_author \
    .alias("cpa") \
    .join(all_comments.alias("ac"),
          F.col("ac.author_salted") == F.concat(F.col("cpa.author"), F.lit("_"), F.col("cpa.exploded_col")), how="left") \
    .where(F.col("ac.created_utc") > F.col("cpa.first_corrected_utc")) \
    .select(F.col("ac.author").alias("author"),
            F.col("cpa.incorrect_spelling").alias("incorrect_spelling"),
            F.col("cpa.correct_spelling").alias("correct_spelling"),
            F.col("ac.comment_id").alias("comment_id"),
            F.col("ac.body").alias("body")) \
    .dropDuplicates(["author",
                     "incorrect_spelling",
                     "correct_spelling",
                     "comment_id",
                     "body"]) \
    .withColumn("incorrect_in_comment", F.expr("body rlike concat('(?i)', '\\\\b', incorrect_spelling, "
                                               "'\\\\b')")) \
    .withColumn("correct_in_comment", F.expr("body rlike concat('(?i)', '\\\\b', correct_spelling, '\\\\b')")) \
    .withColumn("relevant_comment", F.col("incorrect_in_comment") | F.col("correct_in_comment")) \
    .drop(F.col("body"))

# Retrieve aggregate data for each (author, misspelling, correct spelling) combination:
# - the author's number of comments since the correction
# - the author's number of comments with the correct spelling since correction;
# - the author's number of comments with the incorrect spelling since correction;
# - the author's number of comments with the correct and/or incorrect spelling with correction.
results_per_corrected_author = comments_per_corrected_author \
    .groupBy(F.col("author"), F.col("incorrect_spelling"), F.col("correct_spelling")) \
    .agg(F.count("comment_id").alias("comments_since_correction"),
         F.sum(F.col("correct_in_comment").cast("long")).alias("comments_correct_since_correction"),
         F.sum(F.col("incorrect_in_comment").cast("long")).alias("comments_incorrect_since_correction"),
         F.sum(F.col("relevant_comment").cast("long")).alias("comments_relevant_since_correction"))

# Retrieve, for each incorrect and correct spelling combination:
# - the number of authors that were corrected;
# - the number of authors that posted a "relevant" comment since being corrected;
# - the number of authors that posted the correct (and never the incorrect) spelling since being corrected;
# - the number of authors that posted the incorrect (and never the correct) spelling since being corrected;
# - the ratio between comments with incorrect spelling and the total number of relevant comments for authors that posted
#   the incorrect spelling at least once.
agg_per_word = results_per_corrected_author \
    .groupBy(F.col("incorrect_spelling"), F.col("correct_spelling")) \
    .agg(F.countDistinct(F.col("author")).alias("authors"),
         F.countDistinct(F.when(F.col("comments_relevant_since_correction") > 0, F.col("author"))).alias(
             "authors_relevant"),
         F.countDistinct(F.when(
             (F.col("comments_incorrect_since_correction") == 0) & (F.col("comments_relevant_since_correction") > 0),
             F.col("author"))).alias("authors_only_correct"),
         F.countDistinct(F.when((F.col("comments_incorrect_since_correction") > 0) & (
                 F.col("comments_incorrect_since_correction") == F.col("comments_relevant_since_correction")),
                                F.col("author"))).alias("authors_only_incorrect"),
         F.avg(F.when((F.col("comments_incorrect_since_correction") > 0),
                      F.col("comments_incorrect_since_correction") / F.col(
                          "comments_relevant_since_correction"))).alias(
             "authors_only_incorrect_ratio_incorrect_to_relevant")) \
    .cache()

write_data(agg_per_word, "agg_per_word")
agg_per_word.show()

# Retrieve:
# - the total number of authors;
# - the total number of authors that posted a relevant comment after being corrected;
# - the number of authors that only posted the correct spelling after being corrected;
# - the number of authors that only posted the incorrect spelling after being correct.
agg_total = agg_per_word \
    .select(F.sum(F.col("authors")).alias("authors"),
            F.sum(F.col("authors_relevant")).alias("authors_relevant"),
            F.sum(F.col("authors_only_correct")).alias("authors_only_correct"),
            F.sum(F.col("authors_only_incorrect")).alias("authors_only_incorrect")) \
    .cache()

write_data(agg_total, "agg_total")
agg_total.show()
agg_total.unpersist()
