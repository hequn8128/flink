import sys
import numpy
import pandas
from pandas.plotting import scatter_matrix
import matplotlib.pyplot as plt
import numpy as np
import warnings
#
# url="/Users/hequn.chq/Downloads/iris2.csv"
# dataset = pandas.read_csv(url)
#
# setosa=dataset[dataset['Species']=='Iris-setosa']
# versicolor =dataset[dataset['Species']=='Iris-versicolor']
# virginica =dataset[dataset['Species']=='Iris-virginica']
#
#
# plt.figure()
#
# fig,ax=plt.subplots(1,2,figsize=(21, 10))
#
# # plt.xticks(fontsize=20)
# # plt.yticks(fontsize=20)
#
# setosa.plot(x="SepalLengthCm", y="SepalWidthCm", kind="scatter",ax=ax[0],label='setosa',color='r', fontsize=20)
# versicolor.plot(x="SepalLengthCm",y="SepalWidthCm",kind="scatter",ax=ax[0],label='versicolor',color='b', fontsize=20)
# virginica.plot(x="SepalLengthCm", y="SepalWidthCm", kind="scatter", ax=ax[0], label='virginica', color='g', fontsize=20)
#
# setosa.plot(x="PetalLengthCm", y="PetalWidthCm", kind="scatter",ax=ax[1],label='setosa',color='r', fontsize=20)
# versicolor.plot(x="PetalLengthCm",y="PetalWidthCm",kind="scatter",ax=ax[1],label='versicolor',color='b', fontsize=20)
# virginica.plot(x="PetalLengthCm", y="PetalWidthCm", kind="scatter", ax=ax[1], label='virginica', color='g', fontsize=20)
#
# ax[0].set(title='Sepal comparasion ', ylabel='sepal-width')
# ax[1].set(title='Petal Comparasion',  ylabel='petal-width')
# ax[0].legend(fontsize=20)
# ax[1].legend(fontsize=20)
# ax[0].set_xlabel('SepalLengthCm', fontsize=20)
# ax[0].set_ylabel('SepalWidthCm', fontsize=20)
# ax[0].set_title('Sepal comparasion', fontsize=20)
# # ax[0].title(fontsize=20)
# # ax[1].title(fontsize=20)
# ax[1].set_xlabel('PetalLengthCm', fontsize=20)
# ax[1].set_ylabel('PetalWidthCm', fontsize=20)
# ax[1].set_title('Petal Comparasion', fontsize=20)
#
# plt.show()




url="/Users/hequn.chq/Downloads/kmeans_results.csv"
dataset = pandas.read_csv(url)

setosa=dataset[dataset['Results']==1]
versicolor =dataset[dataset['Results']==2]
virginica =dataset[dataset['Results']==0]


plt.figure()

fig,ax=plt.subplots(1,2,figsize=(21, 10))

# plt.xticks(fontsize=20)
# plt.yticks(fontsize=20)

setosa.plot(x="SepalLengthCm", y="SepalWidthCm", kind="scatter",ax=ax[0],label='setosa',color='r', fontsize=20)
versicolor.plot(x="SepalLengthCm",y="SepalWidthCm",kind="scatter",ax=ax[0],label='versicolor',color='b', fontsize=20)
virginica.plot(x="SepalLengthCm", y="SepalWidthCm", kind="scatter", ax=ax[0], label='virginica', color='g', fontsize=20)

setosa.plot(x="PetalLengthCm", y="PetalWidthCm", kind="scatter",ax=ax[1],label='setosa',color='r', fontsize=20)
versicolor.plot(x="PetalLengthCm",y="PetalWidthCm",kind="scatter",ax=ax[1],label='versicolor',color='b', fontsize=20)
virginica.plot(x="PetalLengthCm", y="PetalWidthCm", kind="scatter", ax=ax[1], label='virginica', color='g', fontsize=20)

ax[0].set(title='Sepal comparasion ', ylabel='sepal-width')
ax[1].set(title='Petal Comparasion',  ylabel='petal-width')
ax[0].legend(fontsize=20)
ax[1].legend(fontsize=20)
ax[0].set_xlabel('SepalLengthCm', fontsize=20)
ax[0].set_ylabel('SepalWidthCm', fontsize=20)
ax[0].set_title('Sepal comparasion', fontsize=20)
# ax[0].title(fontsize=20)
# ax[1].title(fontsize=20)
ax[1].set_xlabel('PetalLengthCm', fontsize=20)
ax[1].set_ylabel('PetalWidthCm', fontsize=20)
ax[1].set_title('Petal Comparasion', fontsize=20)

plt.show()
