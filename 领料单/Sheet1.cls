VERSION 1.0 CLASS
BEGIN
  MultiUse = -1  'True
END
Attribute VB_Name = "Sheet1"
Attribute VB_GlobalNameSpace = False
Attribute VB_Creatable = False
Attribute VB_PredeclaredId = True
Attribute VB_Exposed = True
'系统事件 点击页中的超链接后触发
Private Sub Worksheet_FollowHyperlink(ByVal Target As Hyperlink)
    Dim col As Integer
    
    '如果触发超链对象的值为“计算”,调用领料单的计算逻辑
    If Target.Range.Value = "计算" Then
        '获得计算的日期
        col = Target.Range.Column
        col = col - 3
        '调用计算逻辑
        modAction.computeSupplyList_Action col
        
    End If
    
End Sub

'Private Sub Worksheet_SelectionChange(ByVal Target As Range)
'    Dim col As Integer
'
'    If Target.Count > 1 Then
'        Exit Sub
'    End If
'
'    If Target.Value = "计算" Then
'        col = Target.Column
'        col = col - 1
'
'        modAction.computeSupplyList_Action col
'
'    End If
'End Sub

