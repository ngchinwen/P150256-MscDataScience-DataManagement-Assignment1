### 📘 Data Management Assignment 1

<div style="width:80%; text-align:center;">  
<a href="https://business.yelp.com/data/resources/open-dataset/">  
<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/a/ad/Yelp_Logo.svg/1920px-Yelp_Logo.svg.png">  
</a>  
</div>
 
##  <center> Using Text Mining Analysis to Make Suggestions to Improve the Rating of a Poorly-Rated Business</center>

### 🔍 Overview
* Source: Yelp Reviews (taken from [Yelp Business's](https://business.yelp.com/data/resources/open-dataset/) [Open Dataset](https://business.yelp.com/external-assets/files/Yelp-JSON.zip))
* Dataset files: `yelp_academic_dataset_business.json` and `yelp_academic_dataset_review.json`
* Objective: Text Mining Analysis in R, Data Storage and Query using Hive

---

#### 📖 How to read:
>Preview on github: [ass1.md](https://github.com/ngchinwen/P150256-MscDataScience-DataManagement-Assignment1/blob/main/ass1.md)

OR

>Download R Markdown Notebook (to view in browser): [ass1.html](https://github.com/ngchinwen/P150256-MscDataScience-DataManagement-Assignment1/blob/main/ass1.html)
>


>Download R script to run it on R: [ass1.Rmd](https://github.com/ngchinwen/P150256-MscDataScience-DataManagement-Assignment1/blob/main/ass1.Rmd)

#### This repo also contains:
> - <span style="color:blue">gen_csv</span> (The data of which the bulk of the analysis was done. Obtained from a Hive query and exported as a CSV.)
> - [biz2.py](https://github.com/ngchinwen/P150256-MscDataScience-DataManagement-Assignment1/blob/main/biz2.py): (Spark code to convert `yelp_academic_dataset_business.json` into a Hive table.)
> - [jsonToHive_yelpreviews.py](https://github.com/ngchinwen/P150256-MscDataScience-DataManagement-Assignment1/blob/main/jsonToHive_yelpreviews.py): (Spark code to convert `yelp_academic_dataset_business.json` into a Hive table.)

Additional files: ass1_files, images, and references.json.
>- ass1_files are the output from running the codeblocks
>- images contains screenshots inserted into the notebook
>- references.json is for the references


## ✨ Process and Tools

* R Markdown for narrative + code
* Python integration via `{reticulate}`
* Connect to Hive through Hadoop ecosystem
* Pull large datasets from Hive tables
* Clean, analyse, and visualize data in R

## ⚙️ Setup

### 1. Prerequisites
(Versions as of the project's final date, may not be necessary)
* R (version 4.4.3)
* Python (version 3.8)
* R packages: `reticulate`, `tidyverse`, `ggplot2`, etc.
* Python packages: `pandas`, `pyhive`, etc.
* Hive access
* Hadoop CLI/tools installed (VirtualBox and PuTTY for Windows)

### 2. Set up conda environment
```conda
conda create --name ukm_stqd6324 python=3.8 -y
conda activate ukm_stqd6324 
conda install pandas numpy matplotlib seaborn scikit-learn jupyter
pip install pyhive thrift thrift-sasl pure-sasl impyla
conda install -c conda-forge sasl jupyterlab openpyxl plotly
```

### 3. Connect local port to VirtualBox
```cmd
ssh -L 10000:localhost:10000 maria_dev@127.0.0.1
```

### 4. Follow the rest of the set-up as written in the .md file.



## 🧩 Issues
Aside from the usual issues such as not having the right files or tables inside HDFS or Hive or needing to restart components in Ambari, if running the R script in the .Rmd file causes issues when querying the Hive tables, try running the codeblock that connects you to Hive again.
```python
from impala.dbapi import connect

conn = connect(
    host='127.0.0.1',
    port=10000,
    user='maria_dev',
    database='default',
    auth_mechanism = 'PLAIN'
)
cursor = conn.cursor()
```
