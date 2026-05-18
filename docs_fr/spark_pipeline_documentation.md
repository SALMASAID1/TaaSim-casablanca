# 🚕 TaaSim-Casablanca : Documentation du Pipeline d'Ingénierie de Données Spark

## Aperçu Général
Ce document fournit une explication technique détaillée du **pipeline de génération synthétique de trajets** implémenté dans le notebook `03_data_exploration_enhanced.ipynb`.

L'objectif principal du pipeline est de transformer des traces GPS réelles de taxis de **Porto** (utilisées comme proxy statistique) en un jeu de données synthétique de haute fidélité représentant la **mobilité des petits taxis de Casablanca**. Il sert de base de données d'origine pour le moteur de simulation TaaSim.

---

## 🏗 Architecture & Pile Technique

- **Moteur d'Exécution** : [PySpark 3.5.x](https://spark.apache.org/docs/latest/api/python/index.html) (Principalement utilisé pour le stockage distribué et l'application des schémas de données).
- **Traitement Central** : [NumPy](https://numpy.org/) & [Pandas](https://pandas.pydata.org/) vectorisés (Calcul haute performance sur nœuds uniques).
- **Analyse Spatiale** :
    - [OSMnx](https://osmnx.readthedocs.io/) : Extraction du graphe du réseau routier et calcul d'itinéraires.
    - [GeoPandas](https://geopandas.org/) : Jointures spatiales et gestion des zones.
    - [Shapely](https://shapely.readthedocs.io/) : Opérations géométriques.
- **Visualisation** : [Folium](https://python-visualization.github.io/folium/) (Cartes thermiques/Heatmaps) et [Matplotlib/Seaborn](https://matplotlib.org/) (Tracés statistiques).

---

## ⚙️ Le Moteur de Simulation : Le Modèle Gravitaire

Plutôt qu'un simple rejeu des trajets de Porto, le pipeline utilise un **Modèle Gravitaire Doublement Contraint**.

### 1. La Stratégie
- **Données de Porto** : Utilisées pour calibrer le **paramètre de décroissance de la distance ($\beta$)**. Ce paramètre représente la manière dont la fréquence des trajets diminue à mesure que la distance augmente.
- **Données de Casablanca** : Utilisées pour définir les **Origines et Destinations**.
    - **Attractivité** : Déterminée par la densité de population (RGPH-2024) et les points d'intérêt (POI) d'OpenStreetMap (Gares, Centres commerciaux, Universités, etc.).
    - **Coût** : Défini comme la distance routière ou le temps de trajet entre les zones à Casablanca.

### 2. Cadre Théorique
Le modèle suit le cadre de maximisation de l'entropie établi par **Alan Wilson (1967)** et les variations modernes du modèle de radiation (**Simini et al. 2012**).
- **Décroissance de la distance** : $f(d_{ij}) = e^{-\beta d_{ij}}$ ou $d_{ij}^{-\beta}$.

---

## 🛤 Étapes du Pipeline

### §1. Configuration & Cadrage
Centralise tous les paramètres dans une classe de données `SimulationConfig`.
- **Profils** : `quick` (sous-ensemble rapide) vs `full` (échelle de production).
- **Calibrage Urbain** : Fixe le **facteur de tortuosité ($\tau$) à 1,42**, reflétant le réseau routier non linéaire de Casablanca (plus élevé que celui de Porto qui est à 1,35).

### §2. Ingénierie des Caractéristiques Spatiales (Spatial Feature Engineering)
- **Graphe des Zones** : Télécharge le réseau routier de Casablanca via OSMnx.
- **Enrichissement POI** : Extrait les hôpitaux, marchés et hubs de transport pour pondérer l'attractivité des zones.
- **Construction de la Matrice** : Calcule une matrice de distance Haversine $N \times N$ entre tous les arrondissements de la ville.

### §3. Génération de Trajets (Vectorisée)
- **Échantillonnage OD (Origine-Destination)** : Utilise la matrice gravitaire calibrée pour sélectionner des paires origine-destination.
- **Affectation Spatiale** : Échantillonne des coordonnées $(lat, lon)$ exactes à l'intérieur des boîtes de délimitation (bounding boxes) des zones et les « projette » (snap) sur les nœuds du graphe OSM les plus proches.
- **Calcul d'Itinéraires** : Calcul parallélisé du chemin le plus court par Dijkstra pour 10 % des trajets, avec un repli (fallback) sur la distance Haversine $\times \tau$ pour le reste.

### §4. Logique Temporelle et de Tarification
- **Profil Temporel** : Applique des courbes de demande basées sur l'enquête HACA 2019 (pics matinaux, creux en soirée).
- **Calcul du Tarif** : Suit strictement l'**Arrêté n° 3-71-19 (2024)** :
    - **Tarif Jour** : Prise en charge 2,00 DH + 0,20 DH / 80m.
    - **Tarif Nuit** : Majoration de 50 % (à partir de 20:00).
    - **Tarif Minimum** : 7,50 DH.

---

## 📊 Validation & Assurance Qualité

Le notebook intègre une **suite de validation en 6 volets** :
1. **P1 : Distribution de la Distance** : Comparaison de la décroissance de la distance simulée vs Porto.
2. **P2 : Carte Thermique des Flux OD** : Visualisation de la concentration spatiale.
3. **P3 : Corrélation des Tarifs** : Vérification que les tarifs suivent linéairement la distance (avec le bruit du tarif de nuit).
4. **P4 : Estimateur de Durée** : Contrôle de cohérence sur les vitesses moyennes de la ville.
5. **P5 : Densité Temporelle** : Validation de la courbe de demande sur 24 heures.
6. **P6 : Carte Thermique Interactive** : Vérification que les points chauds s'alignent avec les véritables monuments de Casablanca (ex. Twin Center, Casa-Port).

---

## 📚 Ressources Externes pour Approfondir

### 1. Fondations Théoriques
- **Wilson, A. G. (1967).** *A statistical theory of spatial distribution models.* Transportation Research.
- **Simini, F., et al. (2012).** *A universal model for mobility and migration patterns.* Nature.
- **Kaggle : Porto Taxi Service Trajectory Prediction.** [Lien vers le Dataset](https://www.kaggle.com/c/pkdd-15-predict-taxi-service-trajectory-i).

### 2. Outils Techniques
- **Documentation OSMnx** : [Routage et analyse de graphe](https://osmnx.readthedocs.io/en/stable/osmnx.html).
- **PySpark SQL & DataFrame** : [Guide de démarrage](https://spark.apache.org/docs/latest/api/python/getting_started/index.html).

### 3. Contexte Local (Casablanca)
- **HCP Maroc (RGPH 2024)** : [Portail des données](https://www.hcp.ma/).
- **Bulletin Officiel du Royaume du Maroc** : Références pour les tarifs de transport et les décrets d'aménagement urbain.

---

> [!NOTE]
> Cette documentation fait référence à la version **5.0** du pipeline TaaSim. Pour les modifications, veuillez vous référer aux Dossiers de Décision d'Architecture (ADRs) inclus dans le notebook.
