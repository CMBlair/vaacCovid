prval CC_linear_regression = US_Cases.select("5/2/21")
val CD_linear_regression = US_Deaths.select("5/2/21")




var CC_linear_regression = US_Cases.select("5/2/21")
var CD_linear_regression = US_Deaths.select("5/2/21")



var practice = US.Cases.select("Province_State, 5/2/21")
practice.show() 


train_dataset, test_dataset = finalized_data.randomSplit([0.7,0.3])

train_dataset.describe().show()





from pysaprk.ml.regression import LinearRegression

LinReg = LinearRegression(FeaturesCol = "features", labelCol="Grades")

model = LinReg.fit(train_dataset)

pred = model.evaluate(test_dataset)

pred.predictions.show()










