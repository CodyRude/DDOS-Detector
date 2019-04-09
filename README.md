# DDOS-Detector


This repository contains two PySpark based python scripts.

**__ddos_detector.py__**: A DDOS detector that analyzes Apache logs obtained from a Kafka server. It works by counting the number of requests in a moving window and it writes out to disk any ip address that makes more requests than a set threshold value.

**__load_data.py__**: A script to load Apache logs to a Kafka server. It loads the data in pieces, so the first second of Apache logs are loaded first, then the following second worth of log data. For demonstration purposes, the script may wait between sending batches to the server.
