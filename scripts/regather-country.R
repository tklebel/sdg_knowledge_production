# Issue: we don't have a country mapping for all countries. Presumably, because
# we only did the mapping for affiliations in our set. This is not a huge issue,
# since we have country information for 94.8% of affiliations, but it should be
# 100%. We use the OpenAlex API to fill in those countries that are missing.
# However, unfortunately we cannot look up by MAG id. we need to download all
# affiliations and map then.


library(sparklyr)
library(dplyr)
library(arrow)
library(here)
library(openalexR)

Sys.setenv(SPARK_HOME = "/home/tklebel/spark-3.4.0-bin-hadoop3/")
Sys.setenv(HADOOP_HOME = "/home/hadoop/hadoop-3.3.1")
Sys.setenv(HADOOP_CONF_DIR = "/home/hadoop/hadoop-3.3.1/etc/hadoop")
Sys.setenv(YARN_HOME = "/home/hadoop/hadoop-3.3.1")
Sys.setenv(YARN_CONF_DIR = "/home/hadoop/hadoop-3.3.1/etc/hadoop")
Sys.setenv(JAVA_HOME="/usr/lib/jvm/java-1.11.0-openjdk-amd64")

config <- spark_config()
config$spark.executor.cores <- 5
config$spark.executor.instances <- 20
config$spark.executor.memory <- "20G"
config$spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version <- 2

sc <- spark_connect(master = "yarn", config = config,
                    app_name = "get_country_data")

message("Connection to Spark successful!")

message("Reading the datasets...")

# Overall MAG data
affil_cols <- c("paperid", "authorid", "affiliationid", "authorsequencenumber",
                "originalauthor", "originalaffiliation")

mag_2021_paper_author_affil <- spark_read_csv(
  sc,
  "/tklebel/mag-2021-03-15/PaperAuthorAffiliations.txt",
  name = "mag2021_paper_author_affil", delimiter = "\\t",
  memory = FALSE, header = FALSE, columns = affil_cols)

# Affiliations
affils <- spark_read_csv(sc,
                         "/tklebel/SDG/affiliations_with_country_code.csv",
                         name = "affils")

merged <- mag_2021_paper_author_affil %>%
  distinct(affiliationid) %>%
  left_join(affils) %>%
  collect()

# we can see that we don't have the information for all affiliations
merged %>%
  mutate(has_country = !is.na(country)) %>%
  summarise(country_share = mean(has_country))


# those are the affiliations that we need to look up
missing_country_affil_ids <- merged %>%
  filter(is.na(country)) %>%
  pull(affiliationid)


# Download all

