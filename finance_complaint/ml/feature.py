from pyspark import keyword_only  ## < 2.0 -> pyspark.ml.util.keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param, Params, TypeConverters, HasOutputCols, \
    HasInputCols
# Available in PySpark >= 2.3.0 
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.ml import Estimator
from pyspark.sql import DataFrame
from pyspark.sql.functions import desc
from pyspark.sql.functions import col, abs, when, regexp_replace
from typing import List
from pyspark.sql.types import TimestampType, LongType, FloatType, IntegerType

from typing import Dict


class FrequencyEncoder(Estimator, HasInputCols, HasOutputCols,
                       DefaultParamsReadable, DefaultParamsWritable):

    @keyword_only
    def __init__(self, inputCols: List[str] = None, outputCols: List[str] = None, ):
        super(FrequencyEncoder, self).__init__()
        kwargs = self._input_kwargs
        # self._set(**kwargs)
        self.replace_info: Dict[str, Dict[str, str]] = dict()
        self.setParams(**kwargs)

    def setReplaceInfo(self, replace_info: Dict[str, Dict[str, str]]):
        self.replace_info = replace_info

    def getReplaceInfo(self):
        return self.replace_info

    @keyword_only
    def setParams(self, inputCols: List[str] = None, outputCols: List[str] = None, ):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setInputCols(self, value: List[str]):
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCols(self, value: List[str]):
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCols=value)

    def _fit(self, dataframe: DataFrame):
        input_columns = self.getInputCols()
        output_columns = self.getOutputCols()
        for column, new_column in zip(input_columns, output_columns):
            frequency_of_each_category = dataframe.groupBy(column).count()
            # frequency_of_each_category.show()
            replacement_dict = dict()

            for each_category in frequency_of_each_category.collect():
                # print(each_category)
                current_value = each_category[column]
                new_value = each_category['count']
                replacement_dict[current_value] = str(new_value)

            self.replace_info[new_column] = replacement_dict

        self.setReplaceInfo(replace_info=self.replace_info)
        estimator = FrequencyEncoderModel(inputCols=input_columns, outputCols=output_columns)
        estimator.setReplaceInfo(replace_info=self.replace_info)
        return estimator


class FrequencyEncoderModel(FrequencyEncoder, Transformer):

    def __init__(self, inputCols: List[str] = None, outputCols: List[str] = None, ):
        super(FrequencyEncoderModel, self).__init__(inputCols=inputCols, outputCols=outputCols)

    def _transform(self, dataframe: DataFrame):
        inputCols = self.getInputCols()
        outputCols = self.getOutputCols()
        for in_col, out_col in zip(inputCols, outputCols):
            replacement_values = self.replace_info[out_col]
            # dataframe = dataframe.withColumn(out_col,col(in_col))

            for current_val, new_val in replacement_values.items():
                dataframe = dataframe.withColumn(out_col, when(col(in_col).like(current_val),
                                                               regexp_replace(in_col,
                                                                              current_val,
                                                                              str(new_val)
                                                                              )
                                                               ))
            dataframe = dataframe.withColumn(out_col, col(out_col).cast(LongType()))
        return dataframe


class DerivedFeatureGenerator(Transformer, HasInputCols, HasOutputCols,
                              DefaultParamsReadable, DefaultParamsWritable):

    @keyword_only
    def __init__(self, inputCols: List[str] = None, outputCols: List[str] = None, ):
        super(DerivedFeatureGenerator, self).__init__()
        kwargs = self._input_kwargs
        # self._set(**kwargs)
        self.second_within_day = 60 * 60 * 24
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCols: List[str] = None, outputCols: List[str] = None, ):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setInputCols(self, value: List[str]):
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCols(self, value: List[str]):
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCols=value)

    def _fit(self, dataframe: DataFrame):
        return self

    def _transform(self, dataframe: DataFrame):
        inputCols = self.getInputCols()

        for column in inputCols:
            dataframe = dataframe.withColumn(column,
                                             col(column).cast(TimestampType()))

        dataframe = dataframe.withColumn(self.getOutputCols()[0], abs(
            col(inputCols[1]).cast(LongType()) - col(inputCols[0]).cast(LongType())) / (
                                             self.second_within_day))
        return dataframe


class FrequencyImputer(
    Estimator, HasInputCols, HasOutputCols,
    DefaultParamsReadable, DefaultParamsWritable):
    topCategorys = Param(Params._dummy(), "getTopCategorys", "getTopCategorys",
                         typeConverter=TypeConverters.toListString)

    @keyword_only
    def __init__(self, inputCols: List[str] = None, outputCols: List[str] = None, ):
        super(FrequencyImputer, self).__init__()
        self.topCategorys = Param(self, "topCategorys", "")
        self._setDefault(topCategorys="")
        kwargs = self._input_kwargs
        print(kwargs)
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCols: List[str] = None, outputCols: List[str] = None, ):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setTopCategorys(self, value: List[str]):
        return self._set(topCategorys=value)

    def getTopCategorys(self):
        return self.getOrDefault(self.topCategorys)

    def setInputCols(self, value: List[str]):
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCols=value)

    def setOutputCols(self, value: List[str]):
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCols=value)

    def _fit(self, dataset: DataFrame):
        inputCols = self.getInputCols()
        topCategorys = []
        for column in inputCols:
            categoryCountByDesc = dataset.groupBy(column).count().filter(f'{column} is not null').sort(
                desc('count'))
            topCat = categoryCountByDesc.take(1)[0][column]
            topCategorys.append(topCat)

        self.setTopCategorys(value=topCategorys)

        estimator = FrequencyImputerModel(inputCols=self.getInputCols(),
                                          outputCols=self.getOutputCols())

        estimator.setTopCategorys(value=topCategorys)
        return estimator


class FrequencyImputerModel(FrequencyImputer, Transformer):

    def __init__(self, inputCols: List[str] = None, outputCols: List[str] = None, ):
        super(FrequencyImputerModel, self).__init__(inputCols=inputCols, outputCols=outputCols)

    def _transform(self, dataset: DataFrame):
        topCategorys = self.getTopCategorys()
        outputCols = self.getOutputCols()

        updateMissingValue = dict(zip(outputCols, topCategorys))

        inputCols = self.getInputCols()
        for outputColumn, inputColumn in zip(outputCols, inputCols):
            dataset = dataset.withColumn(outputColumn, col(inputColumn))
            print(dataset.columns)
            print(outputColumn, inputColumn)

        dataset = dataset.na.fill(updateMissingValue)

        return dataset
