make_author_groups <- function(spark_author_paper_affiliations) {
  spark_author_paper_affiliations %>%
    group_by(paperid) %>%
    mutate(paper_author_cat = case_when(
      max(authorsequencenumber) == 1 ~ "single",
      max(authorsequencenumber) == 2 ~ "double",
      TRUE ~ "multi"
    )) %>%
    mutate(author_position = case_when(
      paper_author_cat == "single" ~ "first_author",
      paper_author_cat == "double" & authorsequencenumber == 1 ~ "first_author",
      paper_author_cat == "double" & authorsequencenumber == 2 ~ "last_author",
      paper_author_cat == "multi" & authorsequencenumber == 1 ~ "first_author",
      paper_author_cat == "multi" &
        authorsequencenumber == max(authorsequencenumber, na.rm = TRUE) ~ "last_author",
      TRUE ~ "middle_author"
    ))
}

# approach from https://stackoverflow.com/a/11728547/3149349
cut_quantiles <- function(x) {
  cut(x, breaks = quantile(x, probs = seq(0, 1, by = .2), na.rm = TRUE),
      labels = c("p[0,20]", "p(20,40]", "p(40,60]", "p(60,80]", "p(80,100]"),
      include.lowest = TRUE)
}

cut_quartiles <- function(x) {
  cut(x, breaks = quantile(x, probs = seq(0, 1, by = .25), na.rm = TRUE),
      labels = c("p[0,25]", "p(25,50]", "p(50,75]", "p(75,100]"),
      include.lowest = TRUE)
}

fix_sdg <- function(x) fct_relevel(x, "SDG_13", after = 3)

as_year <- function(num_val) lubridate::ymd(num_val, truncated = 2L)
