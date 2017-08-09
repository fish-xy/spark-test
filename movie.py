#coding=UTF-8
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

user_data = sc.textFile("/data0/libfile/ml-100k/u.user")
user_data.first()

#分别统计用户，性别，职业的个数
#切分返回新的RDD
user_fileds = user_data.map(lambda line : line.split("|"))

#统计用户数
num_user = user_fileds.map(lambda fileds : fileds[0]).count()
#统计性别数
num_genders = user_fileds.map(lambda fileds : fileds[1]).distinct().count()
#统计职业数
num_occupations = user_fileds.map(lambda fileds : fileds[2]).distinct().count()
#统计邮编数
num_zipcodes = user_fileds.map(lambda fileds : fileds[3]).distinct().count()
#返回结果
print "用户数: %d,性别数:%d,职业数：%d，邮编数：%d" %(num_user,num_genders,num_occupations,num_zipcodes)

#获取年龄分布情况
ages = user_fileds.map(lambda x : int(x[1])).collect()
#绘制直方图
plt.hist(ages,bins=20,color='lightblue',normed=True)

