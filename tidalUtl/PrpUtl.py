import numpy as np
import pandas as pd

#ラベルエンコード(文字列→数値)
def Label_encode(train, test, feature_name):
    for f in feature_name:
        lbl = LabelEncoder()
        lbl.fit(list(train[f].values) + list(test[f].values))
        train[f] = lbl.transform(list(train[f].values))
        test[f] = lbl.transform(list(test[f].values))
    
    return train, test



#指定したcolumnと値に応じて欠損値補完を行い、新たにXXX_isnan列を設ける。
def FillnaAndInsertIsnan(DataFrame, ColsAndFillVals):
    dfIsNan = None
    for (col, val) in ColsAndFillVals:
        #欠損値の位置を示すbool列生成
        IsnanSeries = DataFrame[col].isnull()
        #欠損値を補完。
        DataFrame[col] = DataFrame[col].fillna(val)
        #Insert用の欠損値の位置を示すbool列
        if dfIsNan is None:
            dfIsNan = pd.DataFrame(IsnanSeries,columns=[IsnanSeries.name])
        else:
            dfIsNan.insert(len(dfIsNan.columns), col + "_isnan", IsnanSeries)
        
    return DataFrame, dfIsNan