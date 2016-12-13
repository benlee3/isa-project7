from pyspark import SparkContext
import itertools

sc = SparkContext("spark://spark-master:7077", "PopularItems")

data = sc.textFile("/tmp/data/access.log", 2)     # each worker loads a piece of the data file


pairs = data.distinct().map(lambda line: line.split("\t")).sortBy(lambda x: x[1])   # tell each worker to split each line of it's partition
print ("HERE PAIRS>>>>>>>>>>>>>>>>>>>>>>>")
output = pairs.collect()                          # bring the data back to the master node so we can print it out
for pair1, pair2 in output:
    print ("pair part 1: %s part2: %s" % (pair1, pair2))
print ("PAIRS DONE")


groups = pairs.groupByKey()
print ("HERE GROUPS>>>>>>>>>>>>>>>>>>>>>>>")
output = groups.collect()                          # bring the data back to the master node so we can print it out
for pair1, pair2 in output:
	print ("group part 1: %s part2: %s" % (pair1, pair2))
	for c in pair2:
		print("id clicked on: %s" % (c))
print ("GROUPS DONE")


pages = groups.map(lambda line:(line[0],itertools.combinations(line[1], 2)))
print ("HERE MAPS>>>>>>>>>>>>>>>>>>>>>>>")
output = pages.collect()                          # bring the data back to the master node so we can print it out
for pair1, pair2 in output:
	print ("group part 1: %s part2: %s" % (pair1, pair2))
	for c,c1 in pair2:
		print("id clicked on: %s part2: %s" % (c,c1))
print ("MAPS DONE")

flat = pages.values().flatMap(lambda line:line)

tuples = flat.map(lambda line:(line,1))
output = tuples.collect()
for row,c in output:
	print("id clicked on: %s count %d" % (row,c))

counted = tuples.reduceByKey(lambda x,y: x+y)
output = counted.collect()                          # bring the data back to the master node so we can print it out
for pair, count in output:
	print ("tuple %s count %s" % (pair, count))
# print ("Popular items done")

filtered = counted.filter(lambda line: line[1] > 2)
output = filtered.collect()
for pair, count in output:
	print ("%s users who viewed item %s also viewed item %s" % (count, pair[0], pair[1]))
	# print ("tuple %s count %s" % (pair, count))
# print ("Popular items done")

# combs = pages.map(lambda line: (line[0],list(line[1])[0]))
# print ("HERE COMBS>>>>>>>>>>>>>>>>>>>>>>>")
# output = combs.collect()                          # bring the data back to the master node so we can print it out
# for pair1, pair2 in output:
# 	print ("group part 1: %s part2: %s" % (pair1, pair2))
# 	# for c,c1 in pair2:
# 	# 	print("id clicked on: %s part2: %s" % (c,c1))
# print ("MAPS DONE")
#
# switch = combs.map(lambda line: (line[1],line[0]))
#
# step4 = switch.groupByKey()
# print ("HERE STEP 4>>>>>>>>>>>>>>>>>>>>>>>")
# output = step4.collect()                          # bring the data back to the master node so we can print it out
# for pair1, pair2 in output:
# 	print ("group part 1: %s part2: %s" % (pair1, pair2))
# 	for c in pair2:
# 		print("user ID: %s" % (c))
# print ("STEP 4 DONE")
#
# user_count = step4.map(lambda line: (line[1],1))
# count = user_count.reduceByKey(lambda x,y: x+y)
# output = count.collect()                          # bring the data back to the master node so we can print it out
# for user_id, count in output:
# 	print ("user_id %s count %s" % (user_id, count))
# 	for c in user_id:
# 		print("user ID: %s" % (c))
# print ("Popular items done")


###JUST NEED TO RE-MAP TO STEP 5 and then FILTER HERE

sc.stop()
