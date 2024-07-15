---
marp: true
title: Project 2 - ELT CBN app
theme: uncover
paginate: true

---
# Project 2 - Modern ELT - CBN app

Jessica - [JesSchattschneider](https://github.com/JesSchattschneider)

---
# Objective

- Provide an analytical instruments to visualise the oceanographic data collected by two buoys deployed in the New Zealand coast (inspired in the [CBN-lite app](https://cbn-lite.cawthron.org.nz/)).
  
![alt text](image.png)

### Limitations of CBN:

- time range (last 3 days)
- Buoy faults (flagged based on the last record collected by each buoy)

---

# Questions/ Viz products

- When was the collected the last temperature and wind measurements?
- Dashboard with time range selection
- Wind rose for wind data
- Line plot for water temperature

---

# Data sources

| Source name | Source type | Source documentation |
| - | - | - |
| Historical data | local database/csv | water temperature and wind data |
| CBN API | REST API | [https://cbn-api.azurewebsites.net/docs](https://cbn-api.azurewebsites.net/docs) |

The data (wind and temperature measurements) are collected every 15 minutes and the API of these project returns a json file with all records from three days ago until the current moment. It is common that these instruments need maintance.

![width:25cm height:12cm](../images/diagram.png)

---

# Solution architecture

![width:20cm height:12cm](../images/diagram.png)

---
# GIT

- [GIT Repository](https://github.com/JesSchattschneider/project-1-bootcamp-group-1)

- Activity (9 branches, 17 pull requests and ~80 commits)

![width:18cm height:9cm](images/image.png)

---

# Extract

- CSV
- API

---

# Load

- Load the CSV and API to a destination DB (bronze DB)

---

# Transform

**Example API - location attribute**

| Location                                          |
|----------------                                   |
|New York (NYC)                                     |
|Remote (Europe, US)              
|Amsterdam, Berlin, Ghent (EU) On-site/hybrid

---

# Transform

**Example CSV - city and country**

| City          | Country        |
| -------       | -------        |
| Tokyo         | Japan          |
| Shanghai      | China          |

- Standardise location columns - geopy - reverse geocoding

---

# Transform

- jinja - target DB (serving clients)

---

# Docker

![width:18cm height:10cm](images/docker.png)

---

# AWS Production

![width:18cm height:10cm](images/ecs.png)

---

# Results

- Are there remote job opportunities available?
![width:10cm height:4cm](images/result.png)

- What are the most common employment types?
![width:10cm height:5cm](images/result1.png)

---

-  What is the population of the top cities with more job opportunities **(SQL needs review)**?
![width:10cm height:10cm](images/result2.png)