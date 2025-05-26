### 📘 Data Management Assignment 1
![](https://upload.wikimedia.org/wikipedia/commons/thumb/a/ad/Yelp_Logo.svg/1920px-Yelp_Logo.svg.png)


##  <center> Using Text Mining Analysis to Make Suggestions to Improve the Rating of a Poorly-Rated Business</center>

### 🔍 Overview
* Source: Yelp Reviews (taken from [Yelp Business's](https://business.yelp.com/data/resources/open-dataset/) [Open Dataset](https://business.yelp.com/external-assets/files/Yelp-JSON.zip))
* Dataset files: `yelp_academic_dataset_business.json` and `yelp_academic_dataset_review.json`
* Objective: Text Mining Analysis in R, Data Storage and Query using Hive

---

#### 📖 How to read:
>Preview on github: [ass1.md file](https://github.com/ngchinwen/P150256-MscDataScience-DataManagement-Assignment1/blob/main/ass1.md) <br>
>Download R Markdown Notebook (to view in browser): [ass1.html](https://github.com/ngchinwen/P150256-MscDataScience-DataManagement-Assignment1/blob/main/ass1.html) <br>
>
OR
>
>Download R script to run it yourself: [ass1.Rmd](https://github.com/ngchinwen/P150256-MscDataScience-DataManagement-Assignment1/blob/main/ass1.Rmd)

#### This repo also contains:
><span style="color:blue">gen_csv</span> (The data of which the bulk of the analysis was done. Obtained from a Hive query and exported as a CSV.)
>- [biz2.py](https://github.com/ngchinwen/P150256-MscDataScience-DataManagement-Assignment1/blob/main/biz2.py): (Spark code to convert `yelp_academic_dataset_business.json` into a Hive table.)
>- [jsonToHive_yelpreviews.py](https://github.com/ngchinwen/P150256-MscDataScience-DataManagement-Assignment1/blob/main/jsonToHive_yelpreviews.py): (Spark code to convert `yelp_academic_dataset_business.json` into a Hive table.)

Additional files:
ass1_files, images, and references.json.
- ass1_files are the output from running the codeblocks
- images contains screenshots inserted into the notebook
- references.json is for the references



[Process and Tools](#ProcessandTools)
 | [Setup](#setup)
 | [Usage](#usage)
 | [File Structure](#file-structure)
 | [Troubleshooting](#troubleshooting)
 | [License](#license)

---



## ✨ Process and Tools

* R Markdown for narrative + code
* Python integration via `{reticulate}`
* Connects to Hive through Hadoop ecosystem
* Pulls large datasets from Hive tables
* Cleans, analyzes, and visualizes data in R

---

## ⚙️ Setup

### 1. Prerequisites
(Versions as of the project's final date, may not be necessary)
* R (version 4.4.3)
* Python (version 3.8)

```
conda install pandas numpy matplotlib seaborn scikit-learn jupyter
pip install pyhive thrift thrift-sasl pure-sasl impyla
conda install -c conda-forge sasl jupyterlab openpyxl plotly
```

* R packages: `reticulate`, `dplyr`, `ggplot2`, etc.
* Python packages: `pandas`, `pyhive`, etc.
* Hive access and proper credentials
* Hadoop CLI/tools installed

Explain how to set up the R and Python environments, e.g., with `renv` or `conda`.

---

## 📁 File Structure

Example:

```
project-name/
├── your_script.Rmd
├── README.md
├── /data
├── /output
└── /scripts
```

---

## 🧩 Troubleshooting??
* Hive connection errors
* Python environment not detected
* Memory/timeout issues with large Hive queries

---

# This is a Heading h1
## This is a Heading h2
###### This is a Heading h6

## Emphasis

*This text will be italic*  
_This will also be italic_

**This text will be bold**  
__This will also be bold__

_You **can** combine them_

## Lists

### Unordered

* Item 1
* Item 2
* Item 2a
* Item 2b
    * Item 3a
    * Item 3b

### Ordered

1. Item 1
2. Item 2
3. Item 3
    1. Item 3a
    2. Item 3b

## Images

![This is an alt text.](/image/sample.webp "This is a sample image.")

## Links

You may be using [Markdown Live Preview](https://markdownlivepreview.com/).

## Blockquotes

> Markdown is a lightweight markup language with plain-text-formatting syntax, created in 2004 by John Gruber with Aaron Swartz.
>
>> Markdown is often used to format readme files, for writing messages in online discussion forums, and to create rich text using a plain text editor.

## Tables

| Left columns  | Right columns |
| ------------- |:-------------:|
| left foo      | right foo     |
| left bar      | right bar     |
| left baz      | right baz     |

## Blocks of code

```
let message = 'Hello world';
alert(message);
```

## Inline code

This web site is using `markedjs/marked`.
