from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel




def init_spark(name, max_excutors):
    conf = (SparkConf().setAppName(name)
            .set("spark.dynamicAllocation.enabled", "false")
            .set("spark.dynamicAllocation.maxExecutors", str(max_excutors))
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))

    sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel('ERROR')
    sqlContext = HiveContext(sc)
    return sc, sqlContext



sc,_ = init_spark('human', 10)

# Load and parse the data
def parsePoint(line):
    values = [float(x) for x in line.replace(',', ' ').split(' ')]
    return LabeledPoint(values[0], values[1:])

data = sc.textFile("data/mllib/ridge-data/lpsa.data")
parsedData = data.map(parsePoint)

# Build the model
model = LinearRegressionWithSGD.train(parsedData, iterations=100, step=0.00000001)

# Evaluate the model on training data
valuesAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
MSE = valuesAndPreds \
    .map(lambda vp: (vp[0] - vp[1])**2) \
    .reduce(lambda x, y: x + y) / valuesAndPreds.count()
print("Mean Squared Error = " + str(MSE))

# Save and load model
model.save(sc, "target/tmp/pythonLinearRegressionWithSGDModel")
sameModel = LinearRegressionModel.load(sc, "target/tmp/pythonLinearRegressionWithSGDModel")