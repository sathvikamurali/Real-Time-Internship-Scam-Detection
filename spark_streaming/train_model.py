import os
import pickle
import pandas as pd
import psycopg2
import re
import nltk

from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.ensemble import RandomForestClassifier, VotingClassifier
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, classification_report, roc_auc_score, f1_score, confusion_matrix
from imblearn.under_sampling import RandomUnderSampler
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

# Download NLTK requirements
nltk.download('punkt')
nltk.download('stopwords')

# PostgreSQL Configuration
DB_CONFIG = {
    'dbname': 'scamjobs',
    'user': 'postgres',
    'password': 'radmin',
    'host': 'localhost',
    'port': '5432'
}


# ----------------------------------------------------------------------
# Load Kaggle dataset from PostgreSQL
# ----------------------------------------------------------------------
def load_kaggle_data():
    print("Loading Kaggle dataset from PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)
    query = """
        SELECT job_title, job_description, fraudulent, data_source
        FROM jobs
        WHERE data_source = 'susjobs_kaggle';
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    print(f"Loaded {len(df)} records from Kaggle dataset.")
    return df


# ----------------------------------------------------------------------
# Text Cleaning
# ----------------------------------------------------------------------
def clean_text(text):
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = re.sub(r'[^a-zA-Z\s]', '', text)
    tokens = word_tokenize(text)
    stop_words = set(stopwords.words('english'))
    tokens = [word for word in tokens if word not in stop_words]
    return " ".join(tokens)


# ----------------------------------------------------------------------
# Preprocessing
# ----------------------------------------------------------------------
def preprocess_data(df):
    print("Preprocessing text data...")
    df['text'] = df['job_title'].fillna('') + " " + df['job_description'].fillna('')
    df['text'] = df['text'].apply(clean_text)
    df['label'] = df['fraudulent'].astype(int)
    return df[['text', 'label']]


# ----------------------------------------------------------------------
# Balance dataset 1:3 ratio
# ----------------------------------------------------------------------
def balance_data(X, y):
    print("Applying Random UnderSampling (1:3 ratio)...")
    rus = RandomUnderSampler(sampling_strategy=0.33, random_state=42)
    X_resampled, y_resampled = rus.fit_resample(X.values.reshape(-1, 1), y)
    X_resampled = X_resampled.flatten()
    print(f"Balanced dataset: {len(y_resampled)} samples | Fraudulent: {sum(y_resampled)} | Legit: {len(y_resampled)-sum(y_resampled)}")
    return X_resampled, y_resampled


# ----------------------------------------------------------------------
# Train Ensemble (Naive Bayes + Random Forest + XGBoost + Logistic Regression)
# ----------------------------------------------------------------------
def train_ensemble(df):
    X = df['text']
    y = df['label']

    X_resampled, y_resampled = balance_data(X, y)

    X_train, X_test, y_train, y_test = train_test_split(
        X_resampled, y_resampled, test_size=0.2, random_state=42, stratify=y_resampled
    )

    print("\nVectorizing text using TF-IDF...")
    vectorizer = TfidfVectorizer(max_features=7000, ngram_range=(1, 2))
    X_train_vec = vectorizer.fit_transform(X_train)
    X_test_vec = vectorizer.transform(X_test)

    print("\nInitializing models...")
    nb = MultinomialNB(alpha=0.5)
    rf = RandomForestClassifier(n_estimators=200, max_depth=15, random_state=42)
    xgb = XGBClassifier(
        n_estimators=300, learning_rate=0.1, max_depth=6, subsample=0.8,
        colsample_bytree=0.8, eval_metric='logloss', random_state=42, n_jobs=-1
    )
    lr = LogisticRegression(max_iter=500, solver='lbfgs', n_jobs=-1)

    # Weighted Voting Ensemble
    ensemble = VotingClassifier(
        estimators=[
            ('nb', nb),
            ('rf', rf),
            ('xgb', xgb),
            ('lr', lr)
        ],
        voting='soft',
        weights=[1, 2, 3, 1],  # XGB and RF get more weight
        n_jobs=-1
    )

    print("\nTraining ensemble model (Naive Bayes + RandomForest + XGBoost + LR)...")
    ensemble.fit(X_train_vec, y_train)

    print("\nEvaluating ensemble performance...")
    y_pred = ensemble.predict(X_test_vec)
    y_prob = ensemble.predict_proba(X_test_vec)[:, 1]

    acc = accuracy_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)
    auc = roc_auc_score(y_test, y_prob)

    print(f"\nAccuracy: {acc:.4f}")
    print(f"F1 Score: {f1:.4f}")
    print(f"AUC-ROC: {auc:.4f}")
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred))
    print("Confusion Matrix:")
    print(confusion_matrix(y_test, y_pred))

    return ensemble, vectorizer


# ----------------------------------------------------------------------
# Save Artifacts (Model + Vectorizer)
# ----------------------------------------------------------------------
def save_artifacts(model, vectorizer):
    os.makedirs(r"C:\tmp", exist_ok=True)
    model_path = r"C:\tmp\ensemble_model.pkl"
    vectorizer_path = r"C:\tmp\tfidf_vectorizer.pkl"

    with open(model_path, 'wb') as f:
        pickle.dump(model, f)
    with open(vectorizer_path, 'wb') as f:
        pickle.dump(vectorizer, f)

    print(f"\nModel saved at: {model_path}")
    print(f"Vectorizer saved at: {vectorizer_path}")


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main():
    print("=" * 70)
    print("TRAINING ENSEMBLE MODEL (Naive Bayes + RF + XGBoost + LR)")
    print("=" * 70)

    df = load_kaggle_data()
    df = preprocess_data(df)
    model, vectorizer = train_ensemble(df)
    save_artifacts(model, vectorizer)

    print("\nTraining complete. Ensemble model ready for deployment.")


if __name__ == "__main__":
    main()