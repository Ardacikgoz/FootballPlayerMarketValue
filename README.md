# Football Player Market Value Prediction

Predicting football players' market values from in-game player attributes using machine learning and deep learning. Transfermarkt market values are used as ground truth and FIFA (sofifa) player ratings as features.

📄 **Paper:** [Machine Learning and Deep Learning Techniques for Football Market Value Prediction](Machine%20Learning%20and%20Deep%20Learning%20Techniques%20for%20FootballMarketValuePrediction.pdf)

## Approach

1. **Data collection** – custom scrapers collect Transfermarkt player IDs and transfer / market value history (`Transfer Data and TransfermarktID Scraper/`).
2. **Preprocessing & matching** (`preprocess.ipynb`) – FIFA ratings and Transfermarkt records are merged per player *and* season. Player and club names come in different formats across sources, so a dedicated name-matching step was built.
3. **Encoding & normalization** (`Normalize_Encode.ipynb`) – one-hot encoding of categorical features (nationality, position, …) and scaling.
4. **Classical models** (`Model.ipynb`) – KNN, linear regression, SVR, random forest and XGBoost regressors, plus PCA and feature selection experiments.
5. **Neural networks** (`neuralNetwork.ipynb`) – basic, deeper and deepest MLP variants with early stopping, dropout and L1/L2 regularization.

## Results

The best neural network and XGBoost models reach **R² ≈ 0.7** with a **mean absolute error of about €1M** on held-out data – see the paper for the full comparison.

## Repository structure

```
├── Transfer Data and TransfermarktID Scraper/   # scrapers + collected CSVs
├── preprocess.ipynb                             # cleaning, merging, name matching
├── Normalize_Encode.ipynb                       # encoding & scaling
├── Model.ipynb                                  # classical ML models
├── neuralNetwork.ipynb                          # deep learning models
└── *.pdf                                        # project paper
```

## Tech stack

pandas · scikit-learn · XGBoost · TensorFlow / Keras · H2O · requests · BeautifulSoup · Selenium (scraping)

## Authors

Arda Açıkgöz · Mehmet Furkan Çalışkan

---
Term project at Hacettepe University, Department of Artificial Intelligence Engineering.
