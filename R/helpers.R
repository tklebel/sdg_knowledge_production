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



# there is a bug in openalexR, see https://github.com/ropensci/openalexR/issues/161
# below is the code from the package with some parts removed so it works for my
# purposes.
custom_import <- function (data, verbose = TRUE)
{
  data <- openalexR:::simple_rapply(data, `%||%`, y = NA)
  if (!is.null(data$id)) {
    data <- list(data)
  }
  n <- length(data)
  pb <- openalexR:::oa_progress(n)
  list_df <- vector(mode = "list", length = n)
  institution_process <- tibble::tribble(~type, ~field, "identical",
                                         "id", "identical", "ror", "identical", "works_api_url",
                                         "identical", "type", "identical", "works_count", "identical",
                                         "display_name", "identical", "country_code", "identical",
                                         "homepage_url", "identical", "image_url", "identical",
                                         "image_thumbnail_url", "identical", "cited_by_count",
                                         "identical", "updated_date", "identical", "created_date",
                                         "identical", "relevance_score", "flat", "display_name_alternatives",
                                         "flat", "display_name_acronyms", "row_df", "geo", "rbind_df",
                                         "counts_by_year", "rbind_df", "x_concepts", "rbind_df",
                                         "associated_institutions", "flat", "ids")
  for (i in seq.int(n)) {
    if (verbose)
      pb$tick()
    item <- data[[i]]
    fields <- institution_process[institution_process$field %in%
                                    names(item), ]
    sim_fields <- mapply(function(x, y) openalexR:::subs_na(item[[x]],
                                                            type = y), fields$field, fields$type, SIMPLIFY = FALSE)

    list_df[[i]] <- c(sim_fields)
  }
  col_order <- c("id", "display_name", "display_name_alternatives",
                 "display_name_acronyms", "display_name_international",
                 "ror", "ids", "country_code", "geo", "type", "homepage_url",
                 "image_url", "image_thumbnail_url", "associated_institutions",
                 "relevance_score", "works_count", "cited_by_count", "counts_by_year",
                 "works_api_url", "x_concepts", "updated_date", "created_date")
  out_df <- openalexR:::rbind_oa_ls(list_df)
  out_df[, intersect(col_order, names(out_df))]
}
