# DDOS-Detector


This repository contains two PySpark based python scripts.

**__ddos_detector.py__**: A DDOS detector that analyzes apache logs obtained from a Kafka server. It works by counting the number of requests in a moving window and it writes out to disk any ip address that makes more requests than a set threshold value.

**__load_data.py__**: An simple script that loads a file containing Apache logs and sends them to a Kafka server.
