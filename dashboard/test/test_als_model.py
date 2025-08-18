from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel

# 1. Start Spark session
spark = SparkSession.builder.appName("ALSModelTest").getOrCreate()

# 2. Load the ALS model
model_path = "als_model"  # relative to dashboard/
als_model = ALSModel.load(model_path)

# check the expected column names
print("userCol:", als_model.userCol)
print("itemCol:", als_model.itemCol)
print("Model params:", als_model.extractParamMap())
print("Model params:", als_model.params)

# 3. Prepare DataFrame with user_id(s) you want recommendations for
# Example: test for user_id = 123
user_id = 123  # <-- change this to your test user_id
users = spark.createDataFrame([(user_id,)], ["user_id"])


# 4. Get top-N recommendations for the user
num_recommendations = 5
user_recs = als_model.recommendForUserSubset(users, num_recommendations)

# 5. Show recommendations
user_recs.show(truncate=False)



recs = user_recs.collect()[0]['recommendations']
product_ids = [r['product_id'] for r in recs]
scores = [r['rating'] for r in recs]
print("Recommended product_ids:", product_ids)
print("Scores:", scores)

# 6. Stop Spark session
spark.stop()