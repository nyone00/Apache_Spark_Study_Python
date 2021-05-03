from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerOrders")
sc = SparkContext(conf = conf)

def customerPricePairs(line):
	fields = line.split(',')
	cs_id = fields[0]
	money_Spent = fields[2]	
	return (int(cs_id), float(money_Spent))


input1 = sc.textFile("./customer-orders.csv")
mappedInput = input1.map(customerPricePairs)
totalSpent = mappedInput.reduceByKey(lambda x, y: x + y)


flipped = totalSpent.map(lambda x:(x[1],x[0]))
totalSpentSorted = flipped.sortByKey()

results = totalSpentSorted.collect()
for results in results:
	print(results)