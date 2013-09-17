Attribute VB_Name = "modAction"
''''''''''''''''''''''''''''''''''''''''''''''''
'事件处理模块
''''''''''''''''''''''''''''''''''''''''''''''''

'初始化按钮触发事件
Public Sub initProdTable_Action()

    Dim res
    
    '确认初始化对话框
    res = MsgBox("是否初始化" & Chr(34) & "日生产批次表" & Chr(34) & "？", vbOKCancel)
    If res <> vbOK Then
        Exit Sub
    End If
    
    'MsgBox "调用初始化过程"
    modDealBiz.dbiz_initProdTable
    
End Sub

'计算领料单按钮触发事件
Public Function computeSupplyList_Action(da As Integer)
    Dim res
    
    '确认领料单计算对话框
    res = MsgBox("开始计算" & da & "日的领料单?", vbOKCancel)
    If res <> vbOK Then
        Exit Function
    End If
    
    'MsgBox "调用计算领料单逻辑"
    modDealBiz.dbiz_computeSupplyList da
    
End Function
