VERSION 1.0 CLASS
BEGIN
  MultiUse = -1  'True
END
Attribute VB_Name = "clsFormulaProdNum"
Attribute VB_GlobalNameSpace = False
Attribute VB_Creatable = False
Attribute VB_PredeclaredId = False
Attribute VB_Exposed = False
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
'生产批次类(用于存放一个配方的生产批次数据及其相关计算)
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Public pfph As String
Private pv_prodNum As New Collection

Public Function addProdNum(ByVal da As Integer, ByVal num As Integer)
    
    pv_prodNum.Add Item:=num, Key:=CStr(da)
    
End Function

'计算指定日的生产批次
Public Function getProdNumofDay(da As Integer) As Integer
    
    getProdNumofDay = pv_prodNum.Item(CStr(da))
    
End Function

'计算指定日之前的生产批次汇总
Public Function getTotalProdNum(da As Integer)
    
    Dim i As Integer
    Dim res As Integer
    res = 0
    
    For i = 1 To da
        res = res + pv_prodNum(i)
    Next
    
    getTotalProdNum = res
    
End Function

