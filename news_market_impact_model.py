from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import (
    classification_report, accuracy_score,
    r2_score, mean_absolute_error,
    mean_squared_error, mean_absolute_percentage_error
)

from transformers import pipeline


# =========================
# 1. DB CONNECTION
# =========================
engine = create_engine("postgresql+psycopg2://user:password@host:5432/dbname")


# =========================
# 2. LOAD DATA
# =========================
articles = pd.read_sql("""
    SELECT *
    FROM articles
""", engine, parse_dates=['publish_dttm'])

quotes = pd.read_sql("""
    SELECT *
    FROM prices
""", engine, parse_dates=['report_dttm'])

quotes = quotes.sort_values('report_dttm')
articles = articles.sort_values('publish_dttm')


# =========================
# 3. PRICE ALIGNMENT (FIXED - NO APPLY)
# =========================

# price at t0
articles = pd.merge_asof(
    articles,
    quotes[['report_dttm', 'close_price']],
    left_on='publish_dttm',
    right_on='report_dttm',
    direction='backward'
).rename(columns={'close_price': 'price_0h'})

# price at t+24h
articles = pd.merge_asof(
    articles,
    quotes[['report_dttm', 'close_price']],
    left_on=articles['publish_dttm'] + pd.Timedelta(hours=24),
    right_on='report_dttm',
    direction='backward'
).rename(columns={'close_price': 'price_24h'})


# =========================
# 4. RETURNS
# =========================
articles['ret_24h'] = (
    articles['price_24h'] - articles['price_0h']
) / articles['price_0h']


# =========================
# 5. TARGET
# =========================
def make_target(x, thr=0.005):
    if x > thr:
        return 1
    elif x < -thr:
        return -1
    return 0

articles['target'] = articles['ret_24h'].apply(make_target)
articles = articles[articles['target'] != 0].copy()
articles = articles.sort_values('publish_dttm')


# =========================
# 6. SPLIT
# =========================
split = int(len(articles) * 0.8)

train = articles.iloc[:split].copy()
test  = articles.iloc[split:].copy()

train['text'] = train['title'].fillna('') + " " + train['full_text'].fillna('')
test['text']  = test['title'].fillna('') + " " + test['full_text'].fillna('')


# =========================
# 7. MARKET FEATURES (FIXED)
# =========================
def add_market(df):
    df = df.sort_values('publish_dttm').set_index('publish_dttm')

    news_flow = df.index.to_series().rolling('24h').count()
    baseline = news_flow.rolling('7d').mean()

    df['news_shock'] = (news_flow / baseline).fillna(1)
    df['momentum_lag'] = df['ret_24h'].shift(1).fillna(0)
    df['volatility'] = df['ret_24h'].shift(1).rolling(10).std().fillna(0)

    return df.reset_index()


train = add_market(train)
test = add_market(test)


# =========================
# 8. TF-IDF
# =========================
tfidf = TfidfVectorizer(
    max_features=5000,
    ngram_range=(1,2),
    stop_words='english'
)

X_train_text = tfidf.fit_transform(train['text'])
X_test_text  = tfidf.transform(test['text'])


# =========================
# 9. FINBERT
# =========================
finbert = pipeline("sentiment-analysis", model="ProsusAI/finbert")

def finbert_score(text):
    if pd.isna(text):
        return 0
    r = finbert(text[:512])[0]
    return r['score'] if r['label'].lower() == 'positive' else -r['score']

train['finbert'] = train['text'].apply(finbert_score)
test['finbert']  = test['text'].apply(finbert_score)


# =========================
# 10. FEATURES
# =========================
features = ['finbert', 'news_shock', 'momentum_lag', 'volatility']

X_train_hybrid = np.hstack([train[features].values, X_train_text.toarray()])
X_test_hybrid  = np.hstack([test[features].values, X_test_text.toarray()])


# =========================
# 11. MODELS
# =========================

# classification
clf = LogisticRegression(max_iter=1000)
clf.fit(X_train_hybrid, train['target'])
pred_class = clf.predict(X_test_hybrid)

# regression
reg = Ridge(alpha=1.0)
reg.fit(X_train_hybrid, train['ret_24h'])
pred_impact = reg.predict(X_test_hybrid)


# =========================
# 12. EVALUATION
# =========================
print("==== CLASSIFICATION ====")
print("Accuracy:", accuracy_score(test['target'], pred_class))
print(classification_report(test['target'], pred_class))

print("\n==== REGRESSION ====")
print("R2:", r2_score(test['ret_24h'], pred_impact))
print("MAE:", mean_absolute_error(test['ret_24h'], pred_impact))
print("RMSE:", np.sqrt(mean_squared_error(test['ret_24h'], pred_impact)))
print("MAPE:", mean_absolute_percentage_error(test['ret_24h'], pred_impact))


# =========================
# 13. RESULTS
# =========================
results = test[['title', 'target', 'ret_24h']].copy()
results['pred_class'] = pred_class
results['impact_score'] = pred_impact

print(results.head(10))


# =========================
# 14. VISUALIZATION
# =========================
train_pred = reg.predict(X_train_hybrid)
test_pred = reg.predict(X_test_hybrid)

plot_train = train.copy()
plot_train['pred'] = train_pred
plot_train['type'] = 'Train'

plot_test = test.copy()
plot_test['pred'] = test_pred
plot_test['type'] = 'Test'

full = pd.concat([plot_train, plot_test])
full['date'] = pd.to_datetime(full['publish_dttm']).dt.date

daily = full.groupby(['date', 'type'])[['ret_24h', 'pred']].mean().reset_index()

plt.figure(figsize=(15,7))

plt.plot(daily['date'], daily['ret_24h'], label='Actual', alpha=0.3)

plt.plot(daily[daily['type']=='Train']['date'],
         daily[daily['type']=='Train']['pred'],
         label='Train Pred')

plt.plot(daily[daily['type']=='Test']['date'],
         daily[daily['type']=='Test']['pred'],
         label='Test Pred')

split_date = daily[daily['type']=='Test']['date'].min()
plt.axvline(split_date, linestyle='--', color='green')

plt.legend()
plt.title("Market Impact Prediction")
plt.grid(True)
plt.show()

# =========================
# SAVE RESULTS TO DB
# =========================

results_df = pd.DataFrame({
    'article_id': test['article_id'].values,
    'score': pred_impact
})

results_df.to_sql(
    'results',
    engine,
    if_exists='append',
    index=False,
    method='multi',
    chunksize=1000
)

print("Results saved to DB: results table")
