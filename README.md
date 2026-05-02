# 🚧 Equipment Monitoring System

---

## Project Description

This project presents a real-time equipment utilization monitoring system designed for construction sites. It automatically determines whether excavators are **ACTIVE** or **INACTIVE** by processing live video streams using a computer vision pipeline.

The system combines object detection with optical flow-based motion analysis to accurately track machine behavior—even under partial occlusion. A region-based motion analysis module ensures correct interpretation of articulated machines (e.g., excavators where the arm moves independently from the tracks).

Results are streamed to an interactive Streamlit dashboard that displays an annotated live video feed, per-machine status cards, and real-time utilization counters.

---

##  Objectives

### Improve Operational Efficiency

Automate equipment monitoring and eliminate manual observation across large construction sites.

### Accurate Activity Classification

Classify machine activities into meaningful states:

* **DIGGING**
* **SWINGING / LOADING**
* **DUMPING**
* **WAITING**

This is achieved using a rule-based state machine driven by motion vectors.

### Real-Time Analytics Dashboard

Provide live insights through:

* Annotated video stream
* Individual machine status cards
* Real-time utilization metrics

###  Data-Driven Cost Optimization

Enable smarter decision-making by:

* Measuring actual working time
* Identifying high-performing equipment
* Supporting performance-based billing

---

## ⚙️ System Pipeline

1. **Video Input** (Live or recorded)
2. **Object Detection Model** (Detect excavators)
3. **Region Extraction** (Focus on machine parts)
4. **Optical Flow Analysis** (Compute motion vectors)
5. **State Machine** (Classify activity)
6. **Dashboard Visualization** (Streamlit interface)

---

##  Features

* Real-time video processing
* Robust detection under occlusion
* Motion-aware activity classification
* Multi-machine tracking
* Interactive dashboard visualization

---

##  Future Improvements

* Integrate deep learning-based action recognition (e.g., 3D CNNs, Transformers)
* Extend support to additional equipment types (cranes, trucks, loaders)
* Add historical analytics and reporting
* Deploy on edge devices for real-time on-site processing

---

## 📊 Use Cases

* Construction site monitoring
* Equipment performance analysis
* Cost optimization and billing systems
* Smart infrastructure solutions

---

