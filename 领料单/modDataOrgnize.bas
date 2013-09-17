Attribute VB_Name = "modDataOrgnize"
'''''''''''''''''''''''''''''''''
'数据组织模块(负责组织页面数据)
'''''''''''''''''''''''''''''''''

'日投料批次页面 数据组织
Public Function dorg_initProdTable(ws As Worksheet, phlist As Collection)
    '本函数被业务处理模块调用，phlist参数存放牌号集合
    
    Dim i As Integer
    Dim j As Integer
    
    ws.Activate

    '在页面添加牌号名称
    ws.Range("A5:A300").Clear
    i = 5
    For Each ph In phlist
        ws.Cells(i, 1).Select
        ws.Cells(i, 1).Value = ph
        
        i = i + 1
    Next
    
    '添加计算按钮 并设置超链接 目标指向单元格自身
    ws.Range("B3:AF3").Clear
    For j = 2 To 32
        With ws
            .Hyperlinks.Add _
                    anchor:=.Cells(3, j), _
                    Address:="", _
                    SubAddress:=.Cells(3, j).Address(RowAbsolute:=False, ColumnAbsolute:=False), _
                    ScreenTip:="点击开始计算", _
                    TextToDisplay:="计算"
        End With
    Next j
    
    ws.Range("A4").Activate
    
End Function

'分牌号的领料表 数据组织
Public Function dorg_generateSupplyTableList(ByRef stl As Collection)
    '本函数由业务处理模块调用,stl为领料单集合
    
    Dim ws As Worksheet
    Dim mb As Range
    Dim i As Integer, i1 As Integer
    Dim st As clsSupplyTable
    Dim stt As clsSupplyTobacco
    
    '定义目标页面
    Set ws = ThisWorkbook.Worksheets("领料单")
    '定义模板区域
    Set mb = ThisWorkbook.Worksheets("配置").Range("B4:D7")
    
    ws.Cells.Clear
    i = 2
    For Each st In stl '循环遍历集合中的每一个牌号的领料单
        i1 = i
        '复制模板
        mb.Copy
        ws.Activate
        ws.Cells(i, 2).Activate
        ws.Paste
        '更新日期、牌号名称、总投产批次
        ws.Cells(i, 2).Offset(0, 0).Value = st.llnf & "年" & st.llyf & "月" & st.llr & "日"
        ws.Cells(i, 2).Offset(0, 1).Value = st.pfph
        ws.Cells(i, 2).Offset(1, 2).Value = st.llpc
        i = i + 3
        '更新烟叶
        For Each stt In st.supTobaccos
            ws.Cells(i, 2).Offset(0, 0).Value = stt.yy
            ws.Cells(i, 2).Offset(0, 1).Value = stt.pc
            ws.Cells(i, 2).Offset(0, 2).Value = stt.num
            i = i + 1
        Next
        
        ws.Range(Cells(i1, 2), Cells(i - 1, 4)).BorderAround xlContinuous, xlMedium, xlColorIndexAutomatic
        
        i = i + 2
        
    Next
    
End Function

'全牌号的领料表 数据组织
Public Function dorg_generateSupplyTableListT(ByRef st_t As clsSupplyTable)
    Dim ws As Worksheet
    Set ws = ThisWorkbook.Worksheets("领料单")
    
    Dim mb As Range
    Set mb = ThisWorkbook.Worksheets("配置").Range("F4:H5")
    
    Dim stt As clsSupplyTobacco
    
    i = 2
    i1 = i
    '复制模板
    mb.Copy
    ws.Activate
    ws.Cells(i, 6).Activate
    ws.Paste
    '更新日期、牌号名称、总投产批次
    ws.Cells(i, 6).Offset(0, 0).Value = st_t.llnf & "年" & st_t.llyf & "月" & st_t.llr & "日"
    ws.Cells(i, 6).Offset(0, 1).Value = st_t.pfph
    i = i + 2
    '更新烟叶
    For Each stt In st_t.supTobaccos
        ws.Cells(i, 6).Offset(0, 0).Value = stt.yy
        ws.Cells(i, 6).Offset(0, 1).Value = stt.pc
        ws.Cells(i, 6).Offset(0, 2).Value = stt.num
        i = i + 1
    Next
     
    ws.Range(Cells(i1, 6), Cells(i - 1, 8)).BorderAround xlContinuous, xlMedium, xlColorIndexAutomatic
    
End Function

'更新配方表的已领料批次
Public Function dorg_modifySuppliedNum(pfph As String, ByRef bmb As clsFormulaBomb)
    Dim ws As Worksheet
    Set ws = ThisWorkbook.Worksheets(pfph)
    
    Dim r As Integer, c As Integer, x As Integer
    'r = 4 + bmb.tobaccos.Count + 2
    c = 1 + bmb.bombId * 3
    r = 4
    Do
        If ws.Cells(r, c - 2).MergeCells Then
            x = ws.Cells(r, c - 2).MergeArea.Rows.Count
        Else
            x = 1
        End If
        r = r + x
    Loop While ws.Cells(r, c - 2).Value <> ""
    r = r + 1
    
    If (bmb.execBatch >= bmb.endBatch) Then
        ws.Cells(r, c).Value = "已投料完成"
    ElseIf (bmb.execBatch < bmb.endBatch And bmb.execBatch >= bmb.startBatch) Then
        ws.Cells(r, c).Value = "已投料完第" & bmb.execBatch & "批"
    Else
        ws.Cells(r, c).Value = ""
    End If
    
End Function
