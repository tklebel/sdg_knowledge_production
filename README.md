# README

Code and data for an investigation of SDG research, entitled "Investigating 
patterns of knowledge production in research on three UN Sustainable Development
Goals". A prior version of some of the analyses is available from
https://github.com/on-merrit/sdg_analysis.


## Explanation of files
The main analysis file is `article/article.qmd`. The analysis was conducted 
using a Spark cluster. Initial sample selection was conducted using python on a
different cluster. The scripts for these operations are in 
`scripts/01-sample-selection`. The scaffolding for running them is available
from the 
[original repository](https://github.com/on-merrit/ON-MERRIT/tree/master/WP3/Task3.2/spark).


## Data
All data files underlying the analysis are provided in `/data`, except the data 
from the UN (see below). The raw MAG data are not shared due to their large file
size but can be obtained on request.

## Licenses
The Leiden ranking was downloaded from https://zenodo.org/record/4745545 and
is included here for further analysis. The copyright remains with CWTS.

Citation: Van Eck, Nees Jan. (2021). CWTS Leiden Ranking 2020 [Data set]. Zenodo. http://doi.org/10.5281/zenodo.4745545


The UN data on countries came from
https://unstats.un.org/unsd/methodology/m49/overview/.

The license does not permit redistribution
(https://www.un.org/en/about-us/terms-of-use#countries).
