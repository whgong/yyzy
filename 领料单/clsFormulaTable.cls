VERSION 1.0 CLASS
BEGIN
  MultiUse = -1  'True
END
Attribute VB_Name = "clsFormulaTable"
Attribute VB_GlobalNameSpace = False
Attribute VB_Creatable = False
Attribute VB_PredeclaredId = False
Attribute VB_Exposed = False
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
'配方表类(用于存放一配方表的数据及其相关计算)
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Public pfph As String
Public pfnf As Integer
Public pfyf As Integer
Public zpc As Integer

Private pv_bombs As New Collection

Property Get bombs()

    Set bombs = pv_bombs

End Property

Public Sub addOneBomb(ByVal bmb As clsFormulaBomb)

    pv_bombs.Add bmb

End Sub

'起始批次计算
Public Property Get qspc() As Integer
    
    Dim pc As Integer, pctmp As Integer
    Dim bmb As clsFormulaBomb
    
    On Error GoTo ErrorHandle

    pc = pv_bombs(1).startBatch
    
    For Each bmb In pv_bombs
        pc = WorksheetFunction.Min(pc, bmb.startBatch)
    Next
    
    qspc = pc
    Exit Property
ErrorHandle:
    MsgBox "存在不符合规则的bomb"
End Property

'结束批次计算
Public Property Get jspc() As Integer
    Dim pc As Integer, pctmp As Integer
    Dim bmb As clsFormulaBomb
    
    On Error GoTo ErrorHandle

    pc = pv_bombs(1).endBatch
    
    For Each bmb In pv_bombs
        pc = WorksheetFunction.Max(pc, bmb.endBatch)
    Next
    
    jspc = pc
    Exit Property
ErrorHandle:
    MsgBox "存在不符合规则的bomb"
End Property
