# the purpose of the script was to explore the rate of articles published as OA
# across OpenAlex. Unfortunately, the data was not quite useful, since the OA
# rate is much lower than it was reported in previous analyses using solely the
# Unpaywall set on Web of Science or solely Crossref data (Piwowar, state of OA)

library(openalexR)
library(purrr)

base_string <- "https://api.openalex.org/works?group_by=is_oa&filter=publication_year:"

res <- openalexR::oa_request(paste0(base_string, "2015"))

get_counts <- function(x) {
  is_oa <- x[["key_display_name"]]
  count <- x[["count"]]
  tibble(is_oa = is_oa, count = count)
}

map_dfr(res, get_counts)


year_range <- 2000:2020

long_res <- map(year_range, ~oa_request(paste0(base_string, .x)))


full_res <- long_res %>%
  list_flatten() %>%
  map_dfr(get_counts) %>%
  mutate(year = rep(year_range, each = 2))

full_res %>%
  group_by(year) %>%
  mutate(prop = count/sum(count)) %>%
  filter(is_oa == "true")

# this is a dead end: the rates of OA are much lower across all of OpenAlex.
# This is thus not a good comparator.
