
from pyspark import keyword_only  ## < 2.0 -> pyspark.ml.util.keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param, Params, TypeConverters,HasOutputCols,HasInputCols
# Available in PySpark >= 2.3.0 
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.ml import Estimator
from pyspark.sql import DataFrame
from pyspark.sql.functions import desc
from pyspark.sql.functions import  col
from typing import  List
class FrequencyImputer(
    Transformer, Estimator, HasInputCol, HasOutputCol,HasInputCols,HasOutputCols,
    DefaultParamsReadable, DefaultParamsWritable):
    topCategory = Param(Params._dummy(), "getTopCategory", "getTopCategory",
                           typeConverter=TypeConverters.toString)

    topCategorys = Param(Params._dummy(), "getTopCategorys", "getTopCategorys",
                        typeConverter=TypeConverters.toListString)

    @keyword_only
    def __init__(self, inputCol:str=None, outputCol:str=None,inputCols:List[str]=None,outputCols:List[str]=None,):
        super(FrequencyImputer, self).__init__()
        self.topCategory = Param(self, "topCategory", "")
        self.topCategorys = Param(self, "topCategorys", "")
        self._setDefault(topCategory="")
        self._setDefault(topCategorys="")
        kwargs = self._input_kwargs
        print(kwargs)

        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None,inputCols:List[str]=None,outputCols:List[str]=None, ):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setTopCategory(self, value: str):
        return self._set(topCategory=value)

    def setTopCategorys(self, value: List[str]):
        return self._set(topCategorys=value)

    def getTopCategory(self):
        return self.getOrDefault(self.topCategory)

    def getTopCategorys(self):
        return self.getOrDefault(self.topCategorys)

    # Required in Spark >= 3.0
    def setInputCol(self, value):
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setInputCols(self, value:List[str]):
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    # Required in Spark >= 3.0
    def setOutputCol(self, value):
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    def setOutputCols(self, value:List[str]):
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    def _fit(self ,dataset :DataFrame):

        inputCols = self.getInputCols()

        if inputCols is None:

            inputCol = self.getInputCol()

            if inputCol is None:
                raise  Exception("inputCol/inputCols has to be defined")
            categoryCountByDesc = dataset.groupBy(inputCol).count().filter(f'{inputCol} is not null').sort(desc('count'))
            topCat = categoryCountByDesc.take(1)[0][inputCol]

            self.setTopCategory(value=topCat)
        else:

            topCategorys = []
            for column in inputCols:
                categoryCountByDesc = dataset.groupBy(column).count().filter(f'{column} is not null').sort(
                    desc('count'))
                topCat = categoryCountByDesc.take(1)[0][column]
                topCategorys.append(topCat)

            self.setTopCategorys(value=topCategorys)

        return self

    def _transform(self, dataset :DataFrame):



        topCategorys =self.getTopCategorys()

        if self.topCategorys is None:
            topCategory = self.getTopCategory()
            outputCol = self.getOutputCol()

            updateMissingValue = {
                outputCol :topCategory
            }

            dataset = dataset.withColumn(outputCol ,col(self.getInputCol()))
            dataset = dataset.na.fill(updateMissingValue)
        else:
            outputCols = self.getOutputCols()


            updateMissingValue = dict(zip(outputCols,topCategorys))

            inputCols = self.getInputCols()
            for outputColumn,inputColumn in zip(outputCols,inputCols):
                dataset = dataset.withColumn(outputColumn, col(inputColumn))
                print(dataset.columns)
                print(outputColumn,inputColumn)


            dataset = dataset.na.fill(updateMissingValue)

        return dataset

