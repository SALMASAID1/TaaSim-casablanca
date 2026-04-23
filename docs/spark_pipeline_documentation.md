# 🚕 TaaSim-Casablanca: Spark Data Engineering Pipeline Documentation

## Overview
This document provides a detailed technical explanation of the **Synthetic Trip Generation Pipeline** implemented in `03_data_exploration_enhanced.ipynb`. 

The pipeline's primary goal is to transform real-world taxi traces from **Porto** (used as a statistical proxy) into a high-fidelity synthetic dataset representing **Casablanca petit-taxi mobility**. It serves as the data foundation for the TaaSim simulation engine.

---

## 🏗 Architecture & Technology Stack

- **Execution Engine**: [PySpark 3.5.x](https://spark.apache.org/docs/latest/api/python/index.html) (Used primarily for distributed storage and schema enforcement).
- **Core Processing**: Vectorized [NumPy](https://numpy.org/) & [Pandas](https://pandas.pydata.org/) (High-performance computation on single nodes).
- **Spatial Analysis**: 
    - [OSMnx](https://osmnx.readthedocs.io/): Road network graph extraction and routing.
    - [GeoPandas](https://geopandas.org/): Spatial joins and zone management.
    - [Shapely](https://shapely.readthedocs.io/): Geometry operations.
- **Visualization**: [Folium](https://python-visualization.github.io/folium/) (Heatmaps) and [Matplotlib/Seaborn](https://matplotlib.org/) (Statistical plots).

---

## ⚙️ The Simulation Engine: The Gravity Model

Rather than a simple replay of Porto trips, the pipeline uses a **Doubly-Constrained Gravity Model**.

### 1. The Strategy
- **Porto Data**: Used to calibrate the **Distance Decay Parameter ($\beta$)**. This parameter represents how trip frequency decreases as distance increases.
- **Casablanca Data**: Used to define the **Origins and Destinations**. 
    - **Attractiveness**: Determined by population density (RGPH-2024) and Points of Interest (POIs) from OpenStreetMap (Stations, Malls, Universities, etc.).
    - **Cost**: Defined as the road distance/time between zones in Casablanca.

### 2. Theoretical Background
The model follows the entropy-maximizing framework established by **Alan Wilson (1967)** and the modern Radiation Model variations (**Simini et al. 2012**). 
- **Distance Decay**: $f(d_{ij}) = e^{-\beta d_{ij}}$ or $d_{ij}^{-\beta}$.

---

## 🛤 Pipeline Stages

### §1. Configuration & Scoping
Centralizes all parameters in a `SimulationConfig` dataclass.
- **Profiles**: `quick` (subset) vs `full` (production scale).
- **Urban Calibration**: Sets the **Tortuosity Factor ($\tau$) to 1.42**, reflecting Casablanca's non-linear road network (higher than Porto's 1.35).

### §2. Spatial Feature Engineering
- **Zone Graph**: Downloads Casablanca's road network via OSMnx.
- **POI Enrichment**: Extracts hospitals, markets, and transport hubs to weight zone attractiveness.
- **Matrix Build**: Computes an $N \times N$ Haversine distance matrix between all city arrondissements.

### §3. Trip Generation (Vectorized)
- **OD Sampling**: Uses the calibrated Gravity Matrix to select origin-destination pairs.
- **Spatial Assignment**: Samples exact $(lat, lon)$ coordinates within zone bounding boxes and "snaps" them to the nearest OSM graph nodes.
- **Routing**: Parallelized Dijkstra shortest-path computation for 10% of trips, with a fallback to Haversine $\times \tau$ for the remainder.

### §4. Temporal and Fare Logic
- **Temporal Profile**: Applies demand curves based on the HACA 2019 survey (morning peaks, evening troughs).
- **Fare Computation**: Strictly follows the **Arrêté n° 3-71-19 (2024)**:
    - **Day Rate**: Flag-fall 2.00 DH + 0.20 DH / 80m.
    - **Night Rate**: 50% surcharge (starting 20:00).
    - **Minimum Fare**: 7.50 DH.

---

## 📊 Validation & Quality Assurance

The notebook includes a **6-Panel Validation Suite**:
1. **P1: Distance Distribution**: Comparison of simulated vs. Porto distance decay.
2. **P2: OD Flow Heatmap**: Visualizing spatial concentration.
3. **P3: Fare Correlation**: Verifying that fares follow distance linearly (with night-rate noise).
4. **P4: Duration Estimator**: Sanity check on average city speeds.
5. **P5: Temporal Density**: Verifying the 24-hour demand curve.
6. **P6: Interactive Heatmap**: Validating that hotspots align with real Casablanca landmarks (e.g., Twin Center, Casa-Port).

---

## 📚 External Resources for Further Study

### 1. Theoretical Foundations
- **Wilson, A. G. (1967).** *A statistical theory of spatial distribution models.* Transportation Research.
- **Simini, F., et al. (2012).** *A universal model for mobility and migration patterns.* Nature.
- **Kaggle: Porto Taxi Service Trajectory Prediction.** [Dataset link](https://www.kaggle.com/c/pkdd-15-predict-taxi-service-trajectory-i).

### 2. Technical Tools
- **OSMnx Documentation**: [Routing and Graph Analysis](https://osmnx.readthedocs.io/en/stable/osmnx.html).
- **PySpark SQL & DataFrame**: [Guide](https://spark.apache.org/docs/latest/api/python/getting_started/index.html).

### 3. Local Context (Casablanca)
- **HCP Morocco (RGPH 2024)**: [Data Portal](https://www.hcp.ma/).
- **Official Bulletin (Morocco)**: References for transport tariffs and urban planning decrees.

---

> [!NOTE]
> This documentation relates to version **5.0** of the TaaSim pipeline. For changes, please refer to the Architecture Decision Records (ADRs) within the notebook.
