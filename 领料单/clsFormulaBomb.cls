VERSION 1.0 CLASS
BEGIN
  MultiUse = -1  'True
END
Attribute VB_Name = "clsFormulaBomb"
Attribute VB_GlobalNameSpace = False
Attribute VB_Creatable = False
Attribute VB_PredeclaredId = False
Attribute VB_Exposed = False
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
'bomb类(用于存放一个bomb数据及其相关计算)
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Public bombId As Integer
Public startBatch As Integer
Public endBatch As Integer
Public batchCount As Integer
Public execBatch As Integer

Private pv_tobaccoList As New Collection

Public Property Get tobaccos()
    Set tobaccos = pv_tobaccoList
End Property

Public Sub addTobacco(ByVal y As clsFormulaTobacco)
    pv_tobaccoList.Add y
End Sub
