VERSION 1.0 CLASS
BEGIN
  MultiUse = -1  'True
END
Attribute VB_Name = "clsSupplyTable"
Attribute VB_GlobalNameSpace = False
Attribute VB_Creatable = False
Attribute VB_PredeclaredId = False
Attribute VB_Exposed = False
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
'领料表类(用于存放一个领料表的数据及其相关计算)
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Public llnf As Integer
Public llyf As Integer
Public llr As Integer
Public pfph As String
Public llpc As Integer
Public llqspc As Integer
Public lljspc As Integer

Private pv_tbcoList As New Collection

Public Property Get supTobaccos()
    Set supTobaccos = pv_tbcoList
End Property

'烟叶数量添加方法 若集合中无烟叶为新增，否则为更新数量
Public Sub addTobacco(ByVal tbco As clsSupplyTobacco)
    
    Dim yybs As String
    Dim tmpyy As clsSupplyTobacco

    On Error GoTo ErrorHandle
    
    yybs = tbco.yy & "[" & tbco.pc & "]"
    pv_tbcoList.Add Item:=tbco, Key:=tbco.yy & "[" & tbco.pc & "]"
    Exit Sub
ErrorHandle:
    Set tmpyy = pv_tbcoList.Item(yybs)
    tmpyy.num = tmpyy.num + tbco.num
    
End Sub
