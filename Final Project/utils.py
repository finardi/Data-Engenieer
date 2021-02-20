import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, col, isnan, when, count

def print_unique_and_missing(df, name_df='imigration'):
    len_df = df.count()
    cols_names = df.columns
    
    nuniques = df.agg(*(countDistinct(col(c)).alias(c) for 
                        c in df.columns)).rdd.flatMap(lambda x: x).collect()
    
    isnan_or_isnull = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for 
                                 c in df.columns]).rdd.flatMap(lambda x: x).collect()

    for col_, uniq, nan in zip (cols_names, nuniques, isnan_or_isnull):
        if name_df == 'imigration':
            print(f'Column {col_:<8} has {uniq:>7} unique values and {nan/len_df*100:<8.3}% NaN values')
        if name_df == 'airport':
            print(f'Column {col_:<12} has {uniq:>7} unique values and {nan/len_df*100:<4.3}% NaN values')
        if name_df == 'demographics':
            print(f'Column {col_:<22} has {uniq:>4} unique values and {nan/len_df*100:<5.3}% NaN values')
        if name_df == 'temperature':
            print(f'Column {col_:<29} has {uniq:>6} unique values and {nan/len_df*100:<4.3}% NaN values')
            
def spark_shape(self):
    return (self.count(), len(self.columns))
pyspark.sql.dataframe.DataFrame.shape = spark_shape            