# The "Incidenti" project

This is a project I'm doing to explore the dataset about all the car crashes with injured people happened in Italy in the years:

- 2017
- 2018

The main idea is to understand what is the most dangerous thing to do on our street. It seems, for instance that just having 
a walk can be dangerous (pedestrians is one of the categories that have the most injured and deaths).

Some charts are in :
- [2017 images](./images/2017);
- [2018 images](./images/2018);

## Project status
```diff
! Work in progress
```
I'm working on a new and better version of that same project based on the 2018 year dataset.
See this [2018-incidenti_stradali.ipynb](./notebook/2018-incidenti_stradali.ipynb)

## Disclaimer
I do know very well Pandas, Matplotlib and I had a fairly good education in statistics but, as all the programmers, I do bugs.
So beware, I have checked the results as carefully as I can but nevertheless do not take for granted my result, check by yourself my 
code and decide if it it is correct or not.

## The dataset
I downloaded the data from the [Istat](https://www.istat.it/it/) site. Istat is the main Italian statistic institute.

You won't find the dataset in this repository because I do not own it, but you can freely download the data files and all the metadata files  from the Istat site (a registration is required).
  
## Installation and build
Clone this repository and than:

```bash
python3 -m venv venv
source ./venv/bin/activate
pip install -r requirements.txt
```
To run **Jupyter** use this command:
```bash
jupyter notebook --notebook-dir <path to your installation>/Incidenti --port=9191
```

