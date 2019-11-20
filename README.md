# misinfo_diffusion

## Table of Contents

- [About](#about)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [Project Organisation](#project-organization)

## About

Explore the dynamics of microscopic-level misinformation topology based on the latent message and user interaction attributes for the consumption and sharing of misinformation in online social networks.

## Getting Started

### Prerequisites

- anaconda

## Usage

### Running the pipleine for the first time

```bash
make create_environment
```

```bash
conda activate misinfo_diffusion
```

### Update Python Dependencies

After update(s) to the requirement.yml:

```bash
make requirements
```

### Scrap Twitter for Tweets

Search for the occurence of 'Manchester United' and 'Liverpool'

```bash
make topic TOPIC=football QUERY="Manchester United AND Liverpool" DEPTH=3
```

Search for the occurence of 'Manchester United' or 'Liverpool'

```bash
make topic TOPIC=football QUERY="Manchester United OR Liverpool"  DEPTH=3
```

You can construct an advanced query using [Twitter's Standard operators](https://developer.twitter.com/en/docs/tweets/search/guides/standard-operators)

To only resume fetching of retweet without scraping

```bash
make topic TOPIC=football RESUME=True
```

or

```bash
make topic TOPIC=football RESUME=true
```

or

```bash
make topic TOPIC=football RESUME=TRUE
```

### Create Tweet-Retweet-Network

To create network for one topic using undirected graph

```bash
make network TOPICS=football
```

or

```bash
make network TOPICS=football USING=simple
```

The choice of graph depends on the structure of the graph you want to represent.

Which graph class should I use?

| Arguement      | Type       | Self Loop Allowed? | Parallel edges allowed? |
| -------------- | ---------- | ------------------ | ----------------------- |
| simple         | Undirected | No                 | No                      |
| directed       | Directed   | No                 | No                      |
| multi          | Undirected | Yes                | Yes                     |
| multi_directed | Undirected | Yes                | Yes                     |

To create network for multiple topics using multi undirected graph

```bash
make network TOPICS="football fashion music game" USING=multi
```

### Compute Content Analysis of Topics

To compute content analysis for multiple topics

```bash
make content_analysis TOPICS="football fashion music game"
```

To compute content analysis for multiple topics

```bash
make content_analysis TOPICS=football
```

### See topics available in database

Copy and paste the code below into ipython

```code
import os
import pymongo
from src.utils import get_topics_in_db

client = pymongo.MongoClient("localhost", 27017)
db = client['misinformation_diffusion']
topics = get_topics_in_db(db)
topics.keys()
```

## Project Organization

```text
  ├── LICENSE
  ├── Makefile           <- Makefile with commands like `make data` or `make train`
  ├── README.md          <- The top-level README for developers using this project.
  ├── data
  │   ├── external       <- Data from third party sources.
  │   ├── interim        <- Intermediate data that has been transformed.
  │   ├── processed      <- The final, canonical data sets for modeling.
  │   └── raw            <- The original, immutable data dump.
  │
  ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
  │
  ├── models             <- Trained and serialized models, model predictions, or model summaries
  │
  ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
  │                         the creator's initials, and a short `-` delimited description, e.g.
  │                         `1.0-jqp-initial-data-exploration`.
  │
  ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
  │
  ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
  │   └── figures        <- Generated graphics and figures to be used in reporting
  │
  ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
  │                         generated with `pip freeze > requirements.txt`
  │
  ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
  ├── src                <- Source code for use in this project.
  │   ├── __init__.py    <- Makes src a Python module
  │   │
  │   ├── data           <- Scripts to download or generate data
  │   │   └── make_dataset.py
  │   │
  │   ├── features       <- Scripts to turn raw data into features for modeling
  │   │   └── build_features.py
  │   │
  │   ├── models         <- Scripts to train models and then use trained models to make
  │   │   │                 predictions
  │   │   ├── predict_model.py
  │   │   └── train_model.py
  │   │
  │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
  │       └── visualize.py
  │
  └── tox.ini            <- tox file with settings for running tox; see tox.testrun.org
```

--------

Project based on the [cookiecutter data science project template](https://drivendata.github.io/cookiecutter-data-science/).
