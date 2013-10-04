Attribute VB_Name = "modDataOrgnize"
'''''''''''''''''''''''''''''''''
'数据组织模块(负责组织页面数据)
'''''''''''''''''''''''''''''''''

'日投料批次页面 数据组织
Public Function dorg_initProdTable(ws As Worksheet, phlist As Collection)
    '本函数被业务处理模块调用，phlist参数存放牌号集合
    Dim r As Integer, c As Integer
    Dim i As Integer
    Dim j As Integer
    
    r = modGlobalVar.ws1_r1
    c = modGlobalVar.ws1_c1
    
    ws.Activate

    '在页面添加牌号名称
    ws.Range("A5:A300").Clear
    i = r
    For Each ph In phlist
        ws.Cells(i, c).Select
        ws.Cells(i, c).Value = ph
        i = i + 1
    Next
    
    '添加计算按钮 并设置超链接 目标指向单元格自身
    r = modGlobalVar.ws1_r4
    c = modGlobalVar.ws1_c4
    ws.Range("B3:AF3").Clear
    For j = c To (c + 30)
        With ws
            .Hyperlinks.Add _
                    anchor:=.Cells(r, j), _
                    Address:="", _
                    SubAddress:=.Cells(r, j).Address(RowAbsolute:=False, ColumnAbsolute:=False), _
                    ScreenTip:="点击开始计算", _
                    TextToDisplay:="计算"
        End With
    Next j
    
    ws.Range("A4").Activate
    
End Function

'分牌号的领料表 数据组织
Public Function dorg_generateSupplyTableList(ByRef stl As Collection)
    '本函数由业务处理模块调用,stl为领料单集合
    Dim r As Integer, c As Integer
    Dim ws As Worksheet
    Dim mb As Range
    Dim i As Integer, i1 As Integer
    Dim st As clsSupplyTable
    Dim stt As clsSupplyTobacco
    
    '定义目标页面
    Set ws = ThisWorkbook.Worksheets("领料单")
    '定义模板区域
    Set mb = ThisWorkbook.Worksheets("配置").Range("B4:D7")
    
    r = modGlobalVar.ws3_r1
    c = modGlobalVar.ws3_c1
    
    ws.Cells.Clear
    i = r
    For Each st In stl '循环遍历集合中的每一个牌号的领料单
        i1 = i
        '复制模板
        mb.Copy
        ws.Activate
        ws.Cells(i, c).Select
        ws.Cells(i, c).Activate
        ws.Paste
        '更新日期、牌号名称、总投产批次
        ws.Cells(i, c).Offset(0, 0).Value = st.llnf & "年" & st.llyf & "月" & st.llr & "日"
        ws.Cells(i, c).Offset(0, 1).Value = st.pfph
        ws.Cells(i, c).Offset(1, 2).Value = st.llpc
        i = i + 3
        '更新烟叶
        For Each stt In st.supTobaccos
            ws.Cells(i, c).Offset(0, 0).Value = stt.yy
            ws.Cells(i, c).Offset(0, 1).Value = stt.pc
            ws.Cells(i, c).Offset(0, 2).Value = stt.num
            i = i + 1
        Next
        
        ws.Range(Cells(i1, c), Cells(i - 1, c + 2)).BorderAround xlContinuous, xlMedium, xlColorIndexAutomatic
        
        sAddr = ws.Range(Cells(i1 + 3, c + 2), Cells(i - 1, c + 2)).Address(RowAbsolute:=False, ColumnAbsolute:=False)
        ws.Cells(i, c).Value = "合计"
        ws.Cells(i, c + 2).Formula = "=sum(" & sAddr & ")"
        
        i = i + 2
        
    Next
    
End Function

'全牌号的领料表 数据组织
Public Function dorg_generateSupplyTableListT(ByRef st_t As clsSupplyTable)
    Dim r As Integer, c As Integer
    
    Dim ws As Worksheet
    Set ws = ThisWorkbook.Worksheets("领料单")
    
    Dim mb As Range
    Set mb = ThisWorkbook.Worksheets("配置").Range("F4:H5")
    
    Dim stt As clsSupplyTobacco
    
    r = modGlobalVar.ws3_r2
    c = modGlobalVar.ws3_c2
    
    i = r
    i1 = i
    '复制模板
    mb.Copy
    ws.Activate
    ws.Cells(i, c).Select
    ws.Cells(i, c).Activate
    ws.Paste
    '更新日期、牌号名称、总投产批次
    ws.Cells(i, c).Offset(0, 0).Value = st_t.llnf & "年" & st_t.llyf & "月" & st_t.llr & "日"
    ws.Cells(i, c).Offset(0, 1).Value = st_t.pfph
    i = i + 2
    '更新烟叶
    For Each stt In st_t.supTobaccos
        ws.Cells(i, c).Offset(0, 0).Value = stt.yy
        ws.Cells(i, c).Offset(0, 1).Value = stt.pc
        ws.Cells(i, c).Offset(0, 2).Value = stt.num
        i = i + 1
    Next
    
    ws.Range(Cells(i1, c), Cells(i - 1, c + 2)).BorderAround xlContinuous, xlMedium, xlColorIndexAutomatic
    
    sAddr = ws.Range(Cells(i1 + 2, c + 2), Cells(i - 1, c + 2)).Address(RowAbsolute:=False, ColumnAbsolute:=False)
    ws.Cells(i, c).Value = "合计"
    ws.Cells(i, c + 2).Formula = "=sum(" & sAddr & ")"
    
End Function

'更新配方表的已领料批次
Public Function dorg_modifySuppliedNum(pfph As String, ByRef bmb As clsFormulaBomb)
    Dim ro As Integer, co As Integer
    Dim ws As Worksheet
    Set ws = ThisWorkbook.Worksheets(pfph)
    
    Dim r As Integer, c As Integer, x As Integer
    
    ro = modGlobalVar.wsp_r2
    co = modGlobalVar.wsp_c2
    
    'r = 4 + bmb.tobaccos.Count + 2
    c = 1 + bmb.bombId * 3
    r = ro
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

' 月计划批次及领料情况 数据组织
Public Function dorg_prodPlan(ByRef fmtc As Collection)
    
    Dim r As Integer, c1 As Integer, c2 As Integer
    Dim ws As Worksheet
    Dim fmt As clsFormulaTable
    Dim pcAddr As String, sumAddr As String
    
    Set ws = ThisWorkbook.Worksheets("日投料批次")
    r = modGlobalVar.ws1_r1
    c0 = modGlobalVar.ws1_c1 '牌号名称列号
    c1 = modGlobalVar.ws1_c2 '月计划批次列号
    c2 = c1 + 1 '领料情况列号
    cp1 = modGlobalVar.ws1_c3 '1日列号
    cp2 = cp1 + 30 '31日列号
    
    Do
        sumAddr = ws.Range(Cells(r, cp1), Cells(r, cp2)).Address(RowAbsolute:=False, ColumnAbsolute:=False) '批次地址
        pcAddr = ws.Cells(r, c1).Address(RowAbsolute:=False, ColumnAbsolute:=False) '生产计划地址
        Set fmt = fmtc(ws.Cells(r, c0).Value)
        ws.Cells(r, c1).Value = fmt.zpc
        ws.Cells(r, c2).Formula = "=IF(" & pcAddr & "="""","""",IF(SUM(" & sumAddr & ")>" & pcAddr & ",""超出""&(SUM(" & _
                                    sumAddr & ")-" & pcAddr & ")&""批"",IF(SUM(" & sumAddr & ")=" & pcAddr & _
                                    ",""已领料完成"",""剩余""&(" & pcAddr & "-SUM(" & sumAddr & "))&""批"")))"
        r = r + 1
    Loop While ws.Cells(r, c0).Value <> ""
    
End Function
