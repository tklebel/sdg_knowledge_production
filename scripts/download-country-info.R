# Issue: we don't have a country mapping for all countries. Presumably, because
# we only did the mapping for affiliations in our set. This is not a huge issue,
# since we have country information for 94.8% of affiliations, but it should be
# 100%. We use the OpenAlex API to fill in those countries that are missing.
# However, unfortunately we cannot look up by MAG id. we need to download all
# affiliations and map then.

library(dplyr)
library(openalexR)
library(readr)
library(purrr)

# Sys.setenv(openalexR.mailto = "tklebel@know-center.at")
#
# all_affils <- oa_request(
#   "https://api.openalex.org/institutions?select=ids,country_code",
#   verbose = TRUE
# )
#
# write_rds(all_affils, "data/external/all_affils.rds")


# prepare country_codes
country_codes <- countrycode %>%
  select(Alpha2, Alpha3) %>%
  as_tibble()

# read back and clean country information
affils_df <- read_rds("data/external/all_affils.rds") %>%
  custom_import() %>%
  mutate(affiliationid = map_chr(ids, pluck, "mag", .default = "")) %>%
  select(-ids) %>%
  left_join(country_codes, by = join_by(country_code == Alpha2))

affils_df %>%
  write_csv("data/external/OpenAlex_affiliations.csv")
