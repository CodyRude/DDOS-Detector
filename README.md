# DDOS-Detector


This repository contains two PySpark based python scripts.

**__ddos_detector.py__**: A DDOS detector that analyzes apache logs obtained from a Kafka server. It works by counting the number of requests in a moving window and it writes out to disk any ip address that makes more requests than a set threshold value.

**__load_data.py__**: A script to load apache logs to a kafka server. It loads the data in pieces, so the first second of apache logs are loaded first, then the following second worth of log data. For demonstration purposes, the script may wait between seending batches to the server.
