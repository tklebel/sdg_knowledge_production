# Issue: we don't have a country mapping for all countries. Presumably, because
# we only did the mapping for affiliations in our set. This is not a huge issue,
# since we have country information for 94.8% of affiliations, but it should be
# 100%. We use the OpenAlex API to fill in those countries that are missing.
# However, unfortunately we cannot look up by MAG id. we need to download all
# affiliations and map then.

# Downloading was done in "download-country-info.R". The resulting file
# "OpenAlex_affiliations.csv" was then used here to fill in gaps.

library(sparklyr)
library(dplyr)
library(tidyr)
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
                         name = "affils", null_value = "NA")

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


# read from OpenAlex data
oa_affils <- read_csv("data/external/OpenAlex_affiliations.csv",
                      col_types = cols(.default = col_character())) %>%
  drop_na(affiliationid)

oa_affils

joined_affils <- tibble(affiliationid = missing_country_affil_ids) %>%
  left_join(., oa_affils)

joined_affils %>%
  mutate(has_country = !is.na(country_code)) %>%
  summarise(country_share = mean(has_country))
# we are not at 100%, but this increases our coverage

# check if our coverage has any discrepancies with the one coming from OpenAlex
checked_countries <- merged %>%
  left_join(oa_affils) %>%
  filter(!is.na(country), !is.na(Alpha3)) %>%
  mutate(same_country = country == Alpha3)

checked_countries %>%
  summarise(correct_countries = mean(same_country))

checked_countries %>%
  filter(!same_country) %>%
  View()
# there are some discrepancies, but mainly regarding larger corporations, where
# it seems that different rationales for determining the country were used.
# I stick to the countries that I had from MAG for the reason of continuity.
# First, not to deviate too much from the methods I applied throughout, and also
# because it is difficult to develop a rationale on which approach is better/more
# consistent across the board (OpenAlex approach seems to be better on some
# larger affiliations, but not uniformly)


# export new data for further analyses
merged %>%
  filter(is.na(country)) %>%
  left_join(select(oa_affils, affiliationid, Alpha3)) %>%
  select(-country, country = Alpha3) %>%
  write_csv("data/processed/additional_country_information.csv")

spark_disconnect(sc)
