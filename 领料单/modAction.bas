Attribute VB_Name = "modAction"
''''''''''''''''''''''''''''''''''''''''''''''''
'事件处理模块
''''''''''''''''''''''''''''''''''''''''''''''''

'初始化按钮触发事件
Public Sub initProdTable_Action()

    modGlobalVar.globalVarInit
    
    unlockSheet1 '解锁页1

    Dim res
    
    '确认初始化对话框
    res = MsgBox("是否初始化" & Chr(34) & "日生产批次表" & Chr(34) & "？", vbOKCancel)
    If res <> vbOK Then
        Exit Sub
    End If
    
    'MsgBox "调用初始化过程"
    modDealBiz.dbiz_initProdTable
    
    lockSheet1 '锁定页1
    
End Sub

'计算领料单按钮触发事件
Public Function computeSupplyList_Action(da As Integer)
    modGlobalVar.globalVarInit
    
    unlockSheet1 '解锁页1
    
    Dim res
    
    '确认领料单计算对话框
    res = MsgBox("开始计算" & da & "日的领料单?", vbOKCancel)
    If res <> vbOK Then
        Exit Function
    End If
    
    'MsgBox "调用计算领料单逻辑"
    modDealBiz.dbiz_computeSupplyList da
    
    lockSheet1 '锁定页1
    
End Function

'“初始化月计划批次”按钮触发事件
Public Sub initProdPlan_Action()
    modGlobalVar.globalVarInit
    
    unlockSheet1 '解锁页1
    
    modDealBiz.dbiz_prodPlan
    
    MsgBox "初始化成功"
    
    lockSheet1 '锁定页1
    
End Sub

Public Function lockSheet1()

    ThisWorkbook.Worksheets("日投料批次").Protect _
        Password:="qqq111=", _
        DrawingObjects:=True, Contents:=True, Scenarios:=True, _
        AllowInsertingColumns:=True, AllowInsertingRows:=True, _
        AllowInsertingHyperlinks:=True, AllowDeletingColumns:=True, _
        AllowDeletingRows:=True, AllowSorting:=True, AllowFiltering:=True
End Function

Public Function unlockSheet1()
    ThisWorkbook.Worksheets("日投料批次").Protect _
        Password:="qqq111=", _
        DrawingObjects:=False, Contents:=False, Scenarios:=False, _
        AllowInsertingColumns:=False, AllowInsertingRows:=False, _
        AllowInsertingHyperlinks:=False, AllowDeletingColumns:=False, _
        AllowDeletingRows:=False, AllowSorting:=False, AllowFiltering:=False
End Function

