import pyspark
from graphframes import GraphFrame
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.rdd import RDD
from pyspark.sql.functions import rand


def read_data(sparkSession, edges_path = '../data/wiki-topcats.txt', vertices_path = '../data/wiki-topcats-page-names.txt'):
    #For our task
    Edges = sparkSession.read.csv(path= edges_path, sep=' ', comment='#', schema='src INT, dst INT')
    Vertices = sparkSession.read.csv(path= vertices_path, sep=' ', comment='#', schema='id INT, name String')
    myGraph = GraphFrame(Vertices, Edges)

    return Edges, Vertices, myGraph

def clustering(Edges, Vertices, myGraph):
    # assign each node to its own cluster
    labels = Vertices.withColumn("label", Vertices["id"])

    #IN A LOOP, UPDATE LABELS
    def update_score(row):
        #row contains of id, name, label
        id, label = row.id, row.label

        # g.triplets().filter("src.id == yourVerticeId")

        #go through all its neibours

        return 0

    for iter in range(1):
        labels.orderBy(rand()).show(3) #1. SHUFFLE LABELS DATAFRAME FIRST, THEN GO THROUGH ALL VERTEXES AND UPDATE LABELS

        #new_labels = labels.rdd.map(update_score)




if __name__ == "__main__":
    spark = SparkSession.builder.appName('GraphClustering').getOrCreate()
    Edges, Vertices, myGraph = read_data(spark)

    

